package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"go-rec/pkg/fsm"
)

func TestCSV(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		content string
		limit   int
		want    []CSVRecord
	}{
		{
			name:    "taobao header",
			content: "UserID,ItemID,CategoryID,Timestamp\nu1,i9,c3,1710000000\n",
			want:    []CSVRecord{{UserID: "u1", ItemID: "i9", CategoryID: "c3", Timestamp: 1710000000}},
		},
		{
			name:    "amazon four column no header limit",
			content: "u2,i8,5.0,1710000001\nu3,i7,4.0,1710000002\n",
			limit:   1,
			want:    []CSVRecord{{UserID: "u2", ItemID: "i8", CategoryID: "i8", Timestamp: 1710000001}},
		},
		{
			name:    "amazon simplified header",
			content: "reviewerID,asin,category,unixReviewTime\nr1,B0001,book,1710000003\n",
			want:    []CSVRecord{{UserID: "r1", ItemID: "B0001", CategoryID: "book", Timestamp: 1710000003}},
		},
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
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := writeTempCSV(t, tc.content)
			got, err := ReadCSV(path, tc.limit)
			if err != nil {
				t.Fatalf("ReadCSV() error = %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("len=%d want %d got=%+v", len(got), len(tc.want), got)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got[%d]=%+v want %+v", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestLoadPayloadFromRecordParsesByFSM(t *testing.T) {
	t.Parallel()
	payload := PayloadFromRecord(CSVRecord{UserID: "user-42", ItemID: "item-9", CategoryID: "cat-7", Timestamp: 1710000004})
	var req fsm.RerankRequest
	if err := fsm.NewParser().Parse(context.Background(), payload, &req); err != nil {
		t.Fatalf("Parse(payload) error = %v payload=%s", err, string(payload))
	}
	if req.SessionIDLen != 36 || req.CategoryString() != "cat-7" || req.BrandString() == "" || req.VersionStamp != 1710000004 {
		t.Fatalf("parsed request = session=%q category=%q brand=%q version=%d", req.SessionIDString(), req.CategoryString(), req.BrandString(), req.VersionStamp)
	}
}

func TestRunnerStreamsOnlyRequestedLimit(t *testing.T) {
	t.Parallel()
	csvPath := writeTempCSV(t, "1,10,100,pv,1511544070\n2,20,200,pv,1511544071\n3,30,300,pv,1511544072\n4,40,400,pv,1511544073\n")
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stats, err := Run(ctx, RunOptions{URL: srv.URL, CSVPath: csvPath, Concurrency: 2, Limit: 3, Timeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if stats.Total != 3 || calls.Load() != 3 {
		t.Fatalf("stats=%+v calls=%d, want exactly 3 streamed requests", stats, calls.Load())
	}
}

func TestRunner(t *testing.T) {
	t.Parallel()
	csvPath := writeTempCSV(t, "UserID,ItemID,CategoryID,Timestamp\nu1,i1,c1,1710000001\nu2,i2,c2,1710000002\nu3,i3,c3,1710000003\n")
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"session_id":"123e4567-e89b-12d3-a456-426614174000","fallback":false,"results":[{"id":1,"score":1}]}`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stats, err := Run(ctx, RunOptions{URL: srv.URL, CSVPath: csvPath, Concurrency: 4, Limit: 3, Timeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if stats.Total != 3 || stats.Success != 3 || stats.Errors != 0 || stats.QPS <= 0 || stats.P99 <= 0 || calls.Load() != 3 {
		t.Fatalf("stats=%+v calls=%d, want complete successful run", stats, calls.Load())
	}
}

func TestLocalDataSamplesParse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		path string
	}{
		{name: "taobao", path: filepath.Join("..", "..", "data", "UserBehavior.csv")},
		{name: "amazon", path: filepath.Join("..", "..", "data", "ratings_Electronics.csv")},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := os.Stat(tt.path); err != nil {
				t.Skipf("sample data not present: %v", err)
			}
			records, err := ReadCSV(tt.path, 20)
			if err != nil {
				t.Fatalf("ReadCSV(%s) error = %v", tt.path, err)
			}
			if len(records) != 20 {
				t.Fatalf("len(records)=%d want 20", len(records))
			}
			parser := fsm.NewParser()
			for i, rec := range records {
				var req fsm.RerankRequest
				if err := parser.Parse(context.Background(), PayloadFromRecord(rec), &req); err != nil {
					t.Fatalf("record[%d] payload failed FSM parse: rec=%+v err=%v", i, rec, err)
				}
				if req.CategoryLen == 0 || req.BrandLen == 0 || req.VersionStamp != rec.Timestamp {
					t.Fatalf("record[%d] parsed request mismatch: rec=%+v category=%q brand=%q version=%d", i, rec, req.CategoryString(), req.BrandString(), req.VersionStamp)
				}
			}
		})
	}
}

func BenchmarkPayloadFromRecord(b *testing.B) {
	rec := CSVRecord{UserID: "u1", ItemID: "i1", CategoryID: "c1", Timestamp: 1710000001}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = PayloadFromRecord(rec)
	}
}

func writeTempCSV(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.csv")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	return path
}
