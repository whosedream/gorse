//go:build cgo

package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	_ "github.com/marcboeker/go-duckdb"
)

// Product represents a candidate product from DuckDB vector search.
type Product struct {
	ItemID    string
	Title     string
	Category  string
	Price     float64
	ImageURL  string
	Score     float64
	Embedding []float32
}

// SearchResult is a lightweight search result (no embedding) for hot-path
// vector similarity queries. It avoids the heap allocation and parsing cost
// of extracting the full embedding column.
type SearchResult struct {
	ItemID string
	Score  float64
}

// DuckDBClient wraps a DuckDB database connection for vector similarity search.
type DuckDBClient struct {
	db          *sql.DB
	dsn         string
	embeddingMu sync.Mutex
}

// NewDuckDBClient opens a DuckDB database. An empty or ":memory:" dsn
// opens an in-memory database. The caller should Close when done.
func NewDuckDBClient(dsn string) (*DuckDBClient, error) {
	if dsn == "" {
		dsn = ":memory:"
	}
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, err
	}
	// Verify connectivity.
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	// Enable concurrent readers.
	if _, err := db.Exec(`SET threads = 4`); err != nil {
		db.Close()
		return nil, err
	}
	return &DuckDBClient{db: db, dsn: dsn}, nil
}

// Close closes the underlying database connection.
func (c *DuckDBClient) Close() error {
	if c == nil || c.db == nil {
		return nil
	}
	err := c.db.Close()
	c.db = nil
	return err
}

// InitProductCatalog initializes the products table. If csvPath is non-empty,
// it uses read_csv_auto to load data. Otherwise it creates the table with
// preset data. If the table already exists, it does nothing (idempotent).
func (c *DuckDBClient) InitProductCatalog(csvPath string) error {
	if c == nil || c.db == nil {
		return errors.New("storage: client not initialized")
	}

	// Check if products table already exists.
	var exists int
	err := c.db.QueryRow(`SELECT COUNT(*) FROM information_schema.tables WHERE table_name='products'`).Scan(&exists)
	if err != nil {
		return err
	}

	if exists > 0 {
		return nil // Table already exists, idempotent.
	}

	if csvPath != "" {
		return c.initFromCSV(csvPath)
	}
	return c.initBenchmarkData()
}

func (c *DuckDBClient) initFromCSV(csvPath string) error {
	_, err := c.db.Exec(`CREATE TABLE products AS SELECT * FROM read_csv_auto(?)`, csvPath)
	return err
}

// initBenchmarkData creates the products table with 125 products across
// 5 categories (25 each) with FLOAT[1024] embedding vectors. Each category
// has a bias in a dedicated 128-dim segment of the embedding space so that
// vector similarity queries can separate categories.
func (c *DuckDBClient) initBenchmarkData() error {
	_, err := c.db.Exec(`
		CREATE TABLE products (
			item_id VARCHAR,
			title VARCHAR,
			category VARCHAR,
			price DOUBLE,
			image_url VARCHAR,
			embedding FLOAT[1024]
		)
	`)
	if err != nil {
		return err
	}

	categories := []string{"数码电子", "母婴用品", "宠物生活", "运动户外", "食品饮料"}
	categoryPrefixes := map[string]string{
		"数码电子": "dig",
		"母婴用品": "baby",
		"宠物生活": "pet",
		"运动户外": "sport",
		"食品饮料": "food",
	}
	catSegments := map[string][2]int{
		"数码电子": {0, 127},
		"母婴用品": {128, 255},
		"宠物生活": {256, 383},
		"运动户外": {384, 511},
		"食品饮料": {512, 639},
	}

	for _, cat := range categories {
		pre := categoryPrefixes[cat]
		seg := catSegments[cat]
		for i := 0; i < 25; i++ {
			itemID := fmt.Sprintf("%s_%03d", pre, i+1)
			title := fmt.Sprintf("%s精选商品%03d", cat, i+1)
			price := 10.00 + float64(i)*6.50
			imageURL := fmt.Sprintf("https://img.example.com/%s/%03d.jpg", pre, i+1)

			// Deterministic random from item_id hash.
			seed := hashItemID(itemID)
			embedding := make([]float32, 1024)
			for j := range embedding {
				v := xorshiftFloat32(&seed)
				if j >= seg[0] && j <= seg[1] {
					// Category bias: positive shift in segment.
					embedding[j] = 0.05 + v*0.10
				} else if j >= 640 {
					// Upper dims: neutral noise.
					embedding[j] = (v - 0.5) * 0.10
				} else {
					// Non-segment dims: near zero.
					embedding[j] = (v - 0.5) * 0.05
				}
			}

			embLiteral := vectorToSQLLiteral(embedding)
			sql := fmt.Sprintf(`INSERT INTO products (item_id, title, category, price, image_url, embedding) VALUES (?, ?, ?, ?, ?, %s::FLOAT[1024])`, embLiteral)
			if _, err := c.db.Exec(sql, itemID, title, cat, price, imageURL); err != nil {
				return err
			}
		}
	}
	return nil
}

// hashItemID returns a 64-bit hash derived from item_id for deterministic
// random vector generation.
func hashItemID(s string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// xorshiftFloat32 returns a pseudo-random float32 in [0,1) using xorshift64*.
// state is updated in place; the sequence is deterministic given the initial seed.
func xorshiftFloat32(state *uint64) float32 {
	x := *state
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	*state = x
	v := (x * 0x2545F4914F6CDD1D) & 0xFFFFFF
	return float32(v) / float32(0x1000000)
}

// SearchWithIntent performs vector similarity search against the products table.
// It uses array_dot_product(embedding, vector) as the scoring function.
// The embedding column is added (with random initialization) if not present.
//
// When category is non-empty, results are filtered by category.
// Results are ordered by score descending and limited to at most limit rows.
func (c *DuckDBClient) SearchBaseline(ctx context.Context, category string, limit int) ([]Product, error) {
	if c == nil || c.db == nil {
		return nil, errors.New("storage: client not initialized")
	}
	if limit <= 0 {
		limit = 10
	}
	query := `SELECT item_id, title, category, price, image_url, 0.0 AS score FROM products`
	var args []interface{}
	if category != "" {
		query += ` WHERE category = ?`
		args = append(args, category)
	}
	query += ` ORDER BY item_id LIMIT ?`
	args = append(args, limit)
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var products []Product
	for rows.Next() {
		var p Product
		if err := rows.Scan(&p.ItemID, &p.Title, &p.Category, &p.Price, &p.ImageURL, &p.Score); err != nil {
			return nil, err
		}
		products = append(products, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return products, nil
}

func (c *DuckDBClient) SearchWithIntent(ctx context.Context, vector []float32, category string, limit int) ([]Product, error) {
	if c == nil || c.db == nil {
		return nil, errors.New("storage: client not initialized")
	}
	if len(vector) == 0 {
		return nil, errors.New("storage: empty vector")
	}
	if limit <= 0 {
		limit = 10
	}

	// Ensure embedding column exists.
	if err := c.ensureEmbeddingColumn(ctx); err != nil {
		return nil, err
	}

	// Build the vector literal for SQL.
	vecLiteral := vectorToSQLLiteral(vector)

	// Build the query.
	query := `SELECT item_id, title, category, price, image_url,
		embedding,
		array_dot_product(embedding, ` + vecLiteral + `::FLOAT[1024]) AS score
		FROM products`

	var args []interface{}
	if category != "" {
		query += ` WHERE category = ?`
		args = append(args, category)
	}
	query += ` ORDER BY score DESC LIMIT ?`
	args = append(args, limit)

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var products []Product
	for rows.Next() {
		var p Product
		var embRaw interface{}
		if err := rows.Scan(&p.ItemID, &p.Title, &p.Category, &p.Price, &p.ImageURL, &embRaw, &p.Score); err != nil {
			return nil, err
		}
		p.Embedding = parseEmbedding(embRaw)
		products = append(products, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return products, nil
}

// SearchByIntent performs lightweight vector similarity search without
// fetching the embedding column. It returns SearchResult (ItemID + Score)
// to avoid the heap allocation and parsing overhead of the full embedding
// array on the hot path.
func (c *DuckDBClient) SearchByIntent(ctx context.Context, vector []float32, category string, limit int) ([]SearchResult, error) {
	if c == nil || c.db == nil {
		return nil, errors.New("storage: client not initialized")
	}
	if len(vector) == 0 {
		return nil, errors.New("storage: empty vector")
	}
	if limit <= 0 {
		limit = 10
	}

	// Ensure embedding column exists (no-op if already present).
	if err := c.ensureEmbeddingColumn(ctx); err != nil {
		return nil, err
	}

	vecLiteral := vectorToSQLLiteral(vector)

	query := `SELECT item_id,
			array_dot_product(embedding, ` + vecLiteral + `::FLOAT[1024]) AS score
			FROM products`

	var args []interface{}
	if category != "" {
		query += ` WHERE category = ?`
		args = append(args, category)
	}
	query += ` ORDER BY score DESC LIMIT ?`
	args = append(args, limit)

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SearchResult
	for rows.Next() {
		var r SearchResult
		if err := rows.Scan(&r.ItemID, &r.Score); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// ensureEmbeddingColumn adds an embedding column (FLOAT[1024]) with random
// initialization if it does not already exist. Uses a mutex to serialize
// schema changes and avoid race conditions with concurrent access.
func (c *DuckDBClient) ensureEmbeddingColumn(ctx context.Context) error {
	c.embeddingMu.Lock()
	defer c.embeddingMu.Unlock()

	// Check if embedding column already exists.
	var hasCol int
	err := c.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.columns WHERE table_name='products' AND column_name='embedding'`,
	).Scan(&hasCol)
	if err != nil {
		return err
	}
	if hasCol > 0 {
		return nil
	}

	// Add the column. IF NOT EXISTS is supported in DuckDB >= 1.0.
	if _, err := c.db.ExecContext(ctx,
		`ALTER TABLE products ADD COLUMN IF NOT EXISTS embedding FLOAT[1024]`,
	); err != nil {
		// Fallback for older DuckDB versions: check if error is about existing column.
		if strings.Contains(err.Error(), "already exists") {
			return nil
		}
		return err
	}

	// Initialize with random values for rows with NULL embedding.
	// Uses DuckDB's list() + generate_series() to create a 1024-element array.
	if _, err := c.db.ExecContext(ctx, `
		UPDATE products SET embedding = (
			SELECT list(random() - 0.5)
			FROM generate_series(1, 1024)
		)
		WHERE embedding IS NULL
	`); err != nil {
		return err
	}

	return nil
}

// vectorToSQLLiteral converts a float32 slice to a DuckDB FLOAT array literal.
// Format: [v0,v1,v2,...] - safe because all elements are valid float literals.
func vectorToSQLLiteral(vec []float32) string {
	if len(vec) == 0 {
		return "[]"
	}
	// Pre-allocate: each float ~= 15 bytes max, plus comma between each, plus brackets.
	buf := make([]byte, 0, len(vec)*16+2)
	buf = append(buf, '[')
	// Use a small buffer for formatting each float to avoid allocation.
	var tmp [32]byte
	for i, v := range vec {
		if i > 0 {
			buf = append(buf, ',')
		}
		n := formatFloat32(tmp[:], v)
		buf = append(buf, tmp[:n]...)
	}
	buf = append(buf, ']')
	return string(buf)
}

// formatFloat32 formats a float32 into dst without heap allocation.
// Returns the number of bytes written.
func formatFloat32(dst []byte, v float32) int {
	// Use integer representation for speed.
	// Format: [-]d.ddd... with minimal representation.
	if v != v { // NaN
		copy(dst, "'NaN'")
		return 5
	}
	if v > 3.4028234e+38 { // +Inf
		copy(dst, "'Infinity'")
		return 10
	}
	if v < -3.4028234e+38 { // -Inf
		copy(dst, "'-Infinity'")
		return 11
	}
	if v == 0 {
		dst[0] = '0'
		return 1
	}

	// Use scientific notation with 9 significant digits for precision.
	neg := v < 0
	if neg {
		v = -v
	}

	// Determine exponent (base 10).
	var exp int
	if v >= 10 {
		for v >= 10 {
			v /= 10
			exp++
		}
	} else if v < 1 {
		for v < 1 {
			v *= 10
			exp--
		}
	}

	// Now v is in [1, 10). Format as d.dddddddde+xx.
	n := 0
	if neg {
		dst[0] = '-'
		n++
	}

	mantissa := int64(v*1e9 + 0.5)
	// Write mantissa digits.
	digits := [10]byte{}
	nd := 0
	for mantissa > 0 {
		digits[nd] = byte('0' + mantissa%10)
		nd++
		mantissa /= 10
	}
	if nd == 0 {
		digits[0] = '0'
		nd = 1
	}
	// Reverse digits.
	for i, j := 0, nd-1; i < j; i, j = i+1, j-1 {
		digits[i], digits[j] = digits[j], digits[i]
	}

	dst[n] = digits[0]
	n++
	dst[n] = '.'
	n++
	for i := 1; i < nd && n < 20; i++ {
		dst[n] = digits[i]
		n++
	}
	dst[n] = 'e'
	n++
	if exp >= 0 {
		dst[n] = '+'
		n++
	} else {
		dst[n] = '-'
		n++
		exp = -exp
	}
	if exp >= 10 {
		dst[n] = byte('0' + exp/10)
		n++
	}
	dst[n] = byte('0' + exp%10)
	n++

	return n
}

// parseEmbedding converts a DuckDB FLOAT[] value (returned by go-duckdb as
// []interface{} of float64) into a []float32 slice.
func parseEmbedding(raw interface{}) []float32 {
	switch v := raw.(type) {
	case []interface{}:
		if len(v) == 0 {
			return nil
		}
		out := make([]float32, len(v))
		for i, elem := range v {
			switch f := elem.(type) {
			case float64:
				out[i] = float32(f)
			case float32:
				out[i] = f
			case int64:
				out[i] = float32(f)
			default:
				out[i] = 0
			}
		}
		return out
	default:
		return nil
	}
}
