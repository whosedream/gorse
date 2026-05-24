//go:build !duckdb_use_lib

package main

import "go-rec/api"

func initDuckDB() api.ProductSearcher { return nil }
