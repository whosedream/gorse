//go:build cgo

package storage

import (
	"context"
	"database/sql"
	"errors"
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
	return c.initPresetData()
}

func (c *DuckDBClient) initFromCSV(csvPath string) error {
	_, err := c.db.Exec(`CREATE TABLE products AS SELECT * FROM read_csv_auto(?)`, csvPath)
	return err
}

func (c *DuckDBClient) initPresetData() error {
	_, err := c.db.Exec(`
		CREATE TABLE products (
			item_id VARCHAR,
			title VARCHAR,
			category VARCHAR,
			price DOUBLE,
			image_url VARCHAR
		)
	`)
	if err != nil {
		return err
	}

	presets := []struct {
		id, title, cat string
		price          float64
		image          string
	}{
		{"cat_001", "天然有机猫薄荷逗猫棒", "猫咪用品", 19.90, "https://img.alicdn.com/cat/catnip001.jpg"},
		{"cat_002", "智能自动循环逗猫球USB充电", "猫咪用品", 49.90, "https://img.alicdn.com/cat/ball001.jpg"},
		{"cat_003", "全封闭式超大猫砂盆防溅防臭", "猫咪用品", 89.00, "https://img.alicdn.com/cat/litterbox001.jpg"},
		{"phone_001", "华为Mate 70 Pro昆仑玻璃512GB", "手机数码", 6999.00, "https://img.alicdn.com/phone/huawei001.jpg"},
		{"phone_002", "小米15 Ultra徕卡影像16+512G", "手机数码", 6499.00, "https://img.alicdn.com/phone/xiaomi001.jpg"},
		{"phone_003", "OPPO Find X7 Ultra卫星通信版", "手机数码", 5999.00, "https://img.alicdn.com/phone/oppo001.jpg"},
		{"sport_001", "李宁超轻20代碳板跑鞋男款", "运动户外", 399.00, "https://img.alicdn.com/sport/shoe001.jpg"},
		{"sport_002", "安踏C37 3.0软底缓震跑鞋", "运动户外", 349.00, "https://img.alicdn.com/sport/anta001.jpg"},
		{"sport_003", "Keep智能计数跳绳蓝牙APP同步", "运动户外", 79.00, "https://img.alicdn.com/sport/rope001.jpg"},
		{"book_001", "深入理解计算机系统原书第3版", "图书", 99.00, "https://img.alicdn.com/book/csapp001.jpg"},
		{"book_002", "Go语言高级编程源码级剖析", "图书", 79.00, "https://img.alicdn.com/book/goadv001.jpg"},
		{"book_003", "数据库系统内幕分布式存储设计", "图书", 89.00, "https://img.alicdn.com/book/db001.jpg"},
		{"coffee_001", "隅田川进口意式浓缩挂耳黑咖啡24片", "咖啡茶饮", 39.90, "https://img.alicdn.com/coffee/sumi001.jpg"},
		{"coffee_002", "瑞幸精品冻干即溶咖啡粉12颗装", "咖啡茶饮", 49.90, "https://img.alicdn.com/coffee/luckin001.jpg"},
		{"coffee_003", "星巴克佛罗娜研磨咖啡粉250g", "咖啡茶饮", 69.00, "https://img.alicdn.com/coffee/starbucks001.jpg"},
	}

	for _, p := range presets {
		_, err := c.db.Exec(
			`INSERT INTO products (item_id, title, category, price, image_url) VALUES (?, ?, ?, ?, ?)`,
			p.id, p.title, p.cat, p.price, p.image,
		)
		if err != nil {
			return err
		}
	}
	return nil
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
