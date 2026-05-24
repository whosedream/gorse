//go:build duckdb_use_lib

package main

import (
	"context"
	"log"
	"os"

	"go-rec/api"
	"go-rec/pkg/storage"
)

func initDuckDB() api.ProductSearcher {
	var duckDBClient *storage.DuckDBClient
	if _, err := os.Stat("data/taobao_items.csv"); err == nil {
		dsn := envDefault("DUCKDB_DSN", "")
		if client, initErr := storage.NewDuckDBClient(dsn); initErr == nil {
			if loadErr := client.InitProductCatalog("data/taobao_items.csv"); loadErr != nil {
				log.Printf("duckdb catalog load: %v (continuing without DuckDB)", loadErr)
				client.Close()
			} else {
				duckDBClient = client
				log.Printf("duckdb product catalog loaded from data/taobao_items.csv")
			}
		} else {
			log.Printf("duckdb client init: %v (continuing without DuckDB)", initErr)
		}
	} else {
		if client, initErr := storage.NewDuckDBClient(""); initErr == nil {
			if loadErr := client.InitProductCatalog(""); loadErr != nil {
				log.Printf("duckdb preset catalog: %v (continuing without DuckDB)", loadErr)
				client.Close()
			} else {
				duckDBClient = client
				log.Printf("duckdb product catalog initialized with preset data")
			}
		} else {
			log.Printf("duckdb client init: %v (continuing without DuckDB)", initErr)
		}
	}
	if duckDBClient != nil {
		return &duckDBAdapter{client: duckDBClient}
	}
	return nil
}

// duckDBAdapter bridges *storage.DuckDBClient → api.ProductSearcher
type duckDBAdapter struct {
	client *storage.DuckDBClient
}

func (a *duckDBAdapter) SearchWithIntent(
	ctx context.Context, vector []float32, category string, limit int,
) ([]api.ProductResult, error) {
	products, err := a.client.SearchWithIntent(ctx, vector, category, limit)
	if err != nil {
		return nil, err
	}
	out := make([]api.ProductResult, len(products))
	for i, p := range products {
		out[i] = api.ProductResult{
			ItemID:    p.ItemID,
			Title:     p.Title,
			Category:  p.Category,
			Price:     p.Price,
			ImageURL:  p.ImageURL,
			Score:     p.Score,
			Embedding: p.Embedding,
		}
	}
	return out, nil
}
