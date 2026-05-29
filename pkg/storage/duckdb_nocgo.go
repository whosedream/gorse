//go:build !cgo

package storage

import (
	"context"
	"errors"
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
// vector similarity queries.
type SearchResult struct {
	ItemID string
	Score  float64
}

// DuckDBClient is unavailable when CGO is disabled.
type DuckDBClient struct{}

func NewDuckDBClient(string) (*DuckDBClient, error) {
	return nil, errors.New("storage: duckdb requires cgo")
}

func (c *DuckDBClient) Close() error { return nil }

func (c *DuckDBClient) InitProductCatalog(string) error {
	return errors.New("storage: duckdb requires cgo")
}

func (c *DuckDBClient) SearchBaseline(context.Context, string, int) ([]Product, error) {
	return nil, errors.New("storage: duckdb requires cgo")
}

func (c *DuckDBClient) SearchWithIntent(context.Context, []float32, string, int) ([]Product, error) {
	return nil, errors.New("storage: duckdb requires cgo")
}

func (c *DuckDBClient) SearchByIntent(context.Context, []float32, string, int) ([]SearchResult, error) {
	return nil, errors.New("storage: duckdb requires cgo")
}
