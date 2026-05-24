# Gateway Streaming Loadtest Design

## Scope
Build on the existing HTTP gateway and loadtest runner so historical traffic can be replayed from local public-dataset CSV files without loading the full file into memory.

## Dataset mapping
- Taobao `data/UserBehavior.csv` has no header and uses five columns: `UserID,ItemID,CategoryID,Behavior,Timestamp`.
  - Use column 0 as `UserID`.
  - Use column 1 as `ItemID`.
  - Use column 2 as `CategoryID`.
  - Ignore column 3 `Behavior` for now.
  - Use column 4 as `Timestamp`.
- Amazon `data/ratings_Electronics.csv` has no header and uses four columns: `UserID,ItemID,Rating,Timestamp`.
  - Use column 0 as `UserID`.
  - Use column 1 as `ItemID`.
  - Derive category and brand from `ItemID`.
  - Ignore column 2 `Rating` for now.
  - Use column 3 as `Timestamp`.
- Header-based CSV remains supported for small sample files.

## Runner design
`test/loadtest/runner.go` will expose a streaming path for production-sized files:
- Open with `os.Open`.
- Read line-by-line with `bufio.Scanner` or `encoding/csv.Reader`.
- Producer goroutine parses each row, converts it to an FSM-compatible JSON payload, and sends work into a bounded jobs channel.
- Worker goroutines send HTTP POST requests concurrently.
- Memory stays bounded by `Concurrency`, scanner buffer, latency sample storage, and small fixed per-worker payload buffers.
- No `os.ReadFile`, `ioutil.ReadAll`, or full-record `[]CSVRecord` load in the high-volume path.

## Gateway design
`api/handler.go` remains the HTTP entry point:
- Read bounded request body.
- Reuse request state from `pkg/pool.MemoryPool`.
- Send bytes directly to `pkg/fsm.Parser`.
- Use gateway context timeout for cache I/O, anti-drift intent lookup/defaulting, and scorer `RankParallel`.
- Keep `encoding/json` and `reflect` out of the request parsing path.

## Validation
- Unit tests cover Taobao five-column no-header rows and Amazon four-column no-header rows.
- Runner test verifies payloads parse through `pkg/fsm`.
- Streaming test verifies early `Limit` stops without reading the full file.
- Integration test uses `httptest` gateway and real `data/` sample rows when present.
- Final validation runs `go test`, `go test -race`, `go build`, and loadtest against local sample data.
