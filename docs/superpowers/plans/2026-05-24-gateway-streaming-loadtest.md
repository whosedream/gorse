# Gateway Streaming Loadtest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the gateway loadtest runner truly streaming for multi-GB CSV files and validate it against local Taobao/Amazon samples.

**Architecture:** Keep the existing HTTP gateway and runner structure. Replace the runner's full-file `ReadCSV` execution path with a producer/worker streaming pipeline that parses rows and sends HTTP requests without retaining all records. Preserve small-file helper functions for unit tests if useful, but production `Run` must stream.

**Tech Stack:** Go standard library (`bufio`, `net/http`, `sync`, `atomic`), existing `pkg/fsm`, `api`, and `pkg/pool` components.

---

## File Structure

- Modify `test/loadtest/runner.go`: add streaming reader, row mapping for Taobao/Amazon no-header files, bounded producer/worker runner, and stats accumulation.
- Modify `test/loadtest/runner_test.go`: add tests for Taobao five-column rows, Amazon four-column rows, payload parsing, streaming limit behavior, and runner integration.
- Optionally modify `api/handler_test.go`: only if end-to-end sample payload tests need stronger assertions.

---

### Task 1: Dataset row mapping tests

**Files:**
- Modify: `test/loadtest/runner_test.go`

- [ ] **Step 1: Add failing tests for real no-header formats**

Add tests under `TestCSV`:

```go
{
    name:    "taobao five column no header",
    content: "1,2268318,2520377,pv,1511544070\n",
    want:    []CSVRecord{{UserID: "1", ItemID: "2268318", CategoryID: "2520377", Timestamp: 1511544070}},
},
{
    name:    "amazon four column no header",
    content: "AKM1MP6P0OYPR,0132793040,5.0,1365811200\n",
    want:    []CSVRecord{{UserID: "AKM1MP6P0OYPR", ItemID: "0132793040", CategoryID: "0132793040", Timestamp: 1365811200}},
},
```

- [ ] **Step 2: Run the tests and verify RED**

Run:

```powershell
$machinePath = [Environment]::GetEnvironmentVariable('Path', 'Machine'); $userPath = [Environment]::GetEnvironmentVariable('Path', 'User'); $env:Path = "$machinePath;$userPath"; & "D:\Go\bin\go.exe" test -run TestCSV -count=1 ./test/loadtest
```

Expected: FAIL because the current no-header parser treats the Taobao fifth column incorrectly and does not derive Amazon category from item.

- [ ] **Step 3: Implement row mapping**

In `test/loadtest/runner.go`, update `ParseCSVLine` no-header handling:

```go
func ParseCSVLine(line string, header map[string]int) (CSVRecord, error) {
    fields := splitCSVLine(line)
    if len(fields) < 4 {
        return CSVRecord{}, ErrCSVFormat
    }
    if header == nil {
        if len(fields) >= 5 {
            ts, err := strconv.ParseInt(strings.TrimSpace(fields[4]), 10, 64)
            if err != nil {
                return CSVRecord{}, err
            }
            return CSVRecord{UserID: strings.TrimSpace(fields[0]), ItemID: strings.TrimSpace(fields[1]), CategoryID: strings.TrimSpace(fields[2]), Timestamp: ts}, nil
        }
        ts, err := strconv.ParseInt(strings.TrimSpace(fields[3]), 10, 64)
        if err != nil {
            return CSVRecord{}, err
        }
        item := strings.TrimSpace(fields[1])
        return CSVRecord{UserID: strings.TrimSpace(fields[0]), ItemID: item, CategoryID: item, Timestamp: ts}, nil
    }
    // keep existing header path
}
```

- [ ] **Step 4: Run tests and verify GREEN**

Run the same command. Expected: PASS.

---

### Task 2: Streaming Run path

**Files:**
- Modify: `test/loadtest/runner.go`
- Modify: `test/loadtest/runner_test.go`

- [ ] **Step 1: Add streaming limit test**

Add a test that creates a CSV with many rows, runs `Run` with `Limit: 3`, and asserts only three HTTP requests are sent:

```go
func TestRunnerStreamsOnlyRequestedLimit(t *testing.T) {
    path := writeTempCSV(t, "1,10,100,pv,1511544070\n2,20,200,pv,1511544071\n3,30,300,pv,1511544072\n4,40,400,pv,1511544073\n")
    var calls atomic.Int64
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        calls.Add(1)
        w.WriteHeader(http.StatusOK)
    }))
    defer srv.Close()
    stats, err := Run(context.Background(), RunOptions{URL: srv.URL, CSVPath: path, Concurrency: 2, Limit: 3, Timeout: 50 * time.Millisecond})
    if err != nil {
        t.Fatalf("Run() error = %v", err)
    }
    if stats.Total != 3 || calls.Load() != 3 {
        t.Fatalf("stats=%+v calls=%d, want exactly 3 streamed requests", stats, calls.Load())
    }
}
```

- [ ] **Step 2: Run test and verify current behavior**

Run:

```powershell
& "D:\Go\bin\go.exe" test -run TestRunnerStreamsOnlyRequestedLimit -count=1 ./test/loadtest
```

Expected: It may pass by count, but implementation still uses full `ReadCSV`; keep this as regression coverage and proceed to code inspection constraints in Step 5.

- [ ] **Step 3: Replace `Run` with streaming producer/worker pipeline**

Implement a streaming helper:

```go
func streamCSV(ctx context.Context, path string, limit int, out chan<- CSVRecord) (int64, error) {
    f, err := os.Open(path)
    if err != nil {
        return 0, err
    }
    defer f.Close()
    scanner := bufio.NewScanner(f)
    scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
    var header map[string]int
    var total int64
    lineNo := 0
    for scanner.Scan() {
        select {
        case <-ctx.Done():
            return total, ctx.Err()
        default:
        }
        line := strings.TrimSpace(scanner.Text())
        lineNo++
        if line == "" {
            continue
        }
        if lineNo == 1 && looksLikeHeader(line) {
            header = parseHeader(line)
            continue
        }
        rec, err := ParseCSVLine(line, header)
        if err != nil {
            return total, err
        }
        select {
        case <-ctx.Done():
            return total, ctx.Err()
        case out <- rec:
            total++
        }
        if limit > 0 && int(total) >= limit {
            return total, nil
        }
    }
    if err := scanner.Err(); err != nil {
        return total, err
    }
    return total, nil
}
```

Then rewrite `Run` to start workers first, stream records into `jobs`, close `jobs`, wait for workers, and compute stats from only processed latencies.

- [ ] **Step 4: Keep `ReadCSV` as small-file helper only**

`ReadCSV` can remain for tests, but `Run` must not call `ReadCSV`.

- [ ] **Step 5: Verify no full-file APIs in runner**

Run:

```powershell
Select-String -Path "D:\CodeField\Go-Rec\test\loadtest\runner.go" -Pattern "os.ReadFile|ioutil.ReadAll|io.ReadAll|ReadCSV\(opts.CSVPath"
```

Expected: No matches.

- [ ] **Step 6: Run runner tests**

Run:

```powershell
& "D:\Go\bin\go.exe" test -count=1 ./test/loadtest
```

Expected: PASS.

---

### Task 3: Real data smoke tests

**Files:**
- Modify: `test/loadtest/runner_test.go`

- [ ] **Step 1: Add optional local data tests**

Add tests that skip if files are missing:

```go
func TestLocalDataSamplesParse(t *testing.T) {
    tests := []struct{
        name string
        path string
    }{
        {"taobao", "../../data/UserBehavior.csv"},
        {"amazon", "../../data/ratings_Electronics.csv"},
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            if _, err := os.Stat(tt.path); err != nil {
                t.Skipf("sample data not present: %v", err)
            }
            records, err := ReadCSV(tt.path, 20)
            if err != nil {
                t.Fatalf("ReadCSV(%s) error = %v", tt.path, err)
            }
            if len(records) == 0 {
                t.Fatal("expected at least one record")
            }
            for _, rec := range records {
                var req fsm.RerankRequest
                if err := fsm.NewParser().Parse(context.Background(), PayloadFromRecord(rec), &req); err != nil {
                    t.Fatalf("payload failed FSM parse: rec=%+v err=%v", rec, err)
                }
            }
        })
    }
}
```

- [ ] **Step 2: Run tests**

Run:

```powershell
& "D:\Go\bin\go.exe" test -run TestLocalDataSamplesParse -count=1 ./test/loadtest
```

Expected: PASS if data files exist; SKIP if absent.

---

### Task 4: Final verification

**Files:** all touched files.

- [ ] **Step 1: Run targeted tests**

```powershell
& "D:\Go\bin\go.exe" test -count=1 ./api ./test/loadtest ./cmd/server
```

Expected: PASS.

- [ ] **Step 2: Run race tests**

```powershell
$env:Path = "C:\msys64\ucrt64\bin;D:\redPanda\mingw64\bin;$env:Path"; $env:CGO_ENABLED='1'; & "D:\Go\bin\go.exe" test -race -count=1 ./api ./test/loadtest ./pkg/... ./internal/rk/...
```

Expected: PASS.

- [ ] **Step 3: Run build**

```powershell
& "D:\Go\bin\go.exe" build ./...
```

Expected: exit 0.

- [ ] **Step 4: Run benchmarks**

```powershell
& "D:\Go\bin\go.exe" test ./api ./test/loadtest -run '^$' -bench . -benchmem
```

Expected: PASS and printed QPS/P99 related stats in runner tests or CLI output.

---

## Self-Review

- Spec coverage: covers gateway preservation, sample data mapping, streaming runner, bounded memory, and P99/QPS output.
- Placeholder scan: no TODO/TBD placeholders.
- Type consistency: uses existing `CSVRecord`, `RunOptions`, `Stats`, `PayloadFromRecord`, and `fsm.RerankRequest` names.
