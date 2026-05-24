package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrInvalidRunOptions = errors.New("loadtest: invalid run options")
	ErrCSVFormat         = errors.New("loadtest: invalid csv format")
)

type CSVRecord struct {
	UserID     string
	ItemID     string
	CategoryID string
	Timestamp  int64
}

type RunOptions struct {
	URL         string
	CSVPath     string
	Concurrency int
	Limit       int
	Timeout     time.Duration
	MaxSamples  int
}

type Stats struct {
	Total   int64
	Success int64
	Errors  int64
	QPS     float64
	P50     time.Duration
	P95     time.Duration
	P99     time.Duration
}

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
	idxUser, idxItem, idxCat, idxTS := 0, 1, 2, 3
	if header != nil {
		var ok bool
		idxUser, ok = headerIndex(header, "userid", "reviewerid")
		if !ok {
			return CSVRecord{}, ErrCSVFormat
		}
		idxItem, ok = headerIndex(header, "itemid", "asin")
		if !ok {
			return CSVRecord{}, ErrCSVFormat
		}
		idxCat, ok = headerIndex(header, "categoryid", "category")
		if !ok {
			return CSVRecord{}, ErrCSVFormat
		}
		idxTS, ok = headerIndex(header, "timestamp", "unixreviewtime")
		if !ok {
			return CSVRecord{}, ErrCSVFormat
		}
	}
	maxIdx := idxUser
	if idxItem > maxIdx {
		maxIdx = idxItem
	}
	if idxCat > maxIdx {
		maxIdx = idxCat
	}
	if idxTS > maxIdx {
		maxIdx = idxTS
	}
	if maxIdx >= len(fields) {
		return CSVRecord{}, ErrCSVFormat
	}
	ts, err := strconv.ParseInt(strings.TrimSpace(fields[idxTS]), 10, 64)
	if err != nil {
		return CSVRecord{}, err
	}
	return CSVRecord{UserID: strings.TrimSpace(fields[idxUser]), ItemID: strings.TrimSpace(fields[idxItem]), CategoryID: strings.TrimSpace(fields[idxCat]), Timestamp: ts}, nil
}

func ReadCSV(path string, limit int) ([]CSVRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var out []CSVRecord
	var header map[string]int
	lineNo := 0
	for scanner.Scan() {
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
			return nil, err
		}
		out = append(out, rec)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func PayloadFromRecord(r CSVRecord) []byte {
	buf := make([]byte, 0, 160+len(r.CategoryID)+len(r.ItemID))
	return AppendPayload(buf, r)
}

func AppendPayload(dst []byte, r CSVRecord) []byte {
	dst = append(dst, `{"session_id":"`...)
	dst = appendUUID(dst, r.UserID)
	dst = append(dst, `","version_stamp":`...)
	dst = strconv.AppendInt(dst, r.Timestamp, 10)
	dst = append(dst, `,"slots":{"category":"`...)
	dst = appendEscaped(dst, r.CategoryID)
	dst = append(dst, `","brand":"`...)
	dst = appendEscaped(dst, brandFromItem(r.ItemID))
	dst = append(dst, `"}}`...)
	return dst
}

func Run(ctx context.Context, opts RunOptions) (Stats, error) {
	if opts.URL == "" || opts.CSVPath == "" {
		return Stats{}, ErrInvalidRunOptions
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 1
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 25 * time.Millisecond
	}
	if opts.MaxSamples <= 0 {
		opts.MaxSamples = 100000
	}
	sampleCap := opts.MaxSamples
	if opts.Limit > 0 && opts.Limit < sampleCap {
		sampleCap = opts.Limit
	}
	if sampleCap < 0 {
		sampleCap = 0
	}

	jobs := make(chan CSVRecord, opts.Concurrency)
	latencies := make([]time.Duration, sampleCap)
	transport := &http.Transport{
		MaxIdleConns:        opts.Concurrency * 2,
		MaxIdleConnsPerHost: opts.Concurrency * 2,
		MaxConnsPerHost:     opts.Concurrency * 2,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Timeout: opts.Timeout, Transport: transport}
	var sent atomic.Int64
	var samples atomic.Int64
	var success atomic.Int64
	var errorsN atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < opts.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rec := range jobs {
				payload := PayloadFromRecord(rec)
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, opts.URL, bytes.NewReader(payload))
				if err != nil {
					errorsN.Add(1)
					continue
				}
				req.Header.Set("Content-Type", "application/json")
				t0 := time.Now()
				resp, err := client.Do(req)
				elapsed := time.Since(t0)
				pos := int(samples.Add(1) - 1)
				if pos < len(latencies) {
					latencies[pos] = elapsed
				}
				if err != nil {
					errorsN.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					success.Add(1)
				} else {
					errorsN.Add(1)
				}
			}
		}()
	}
	streamErr := streamCSV(ctx, opts.CSVPath, opts.Limit, jobs, &sent)
	close(jobs)
	wg.Wait()
	stats := buildStats(sent.Load(), success.Load(), errorsN.Load(), start, latencies)
	if streamErr != nil {
		return stats, streamErr
	}
	return stats, nil
}

func streamCSV(ctx context.Context, path string, limit int, out chan<- CSVRecord, sent *atomic.Int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var header map[string]int
	lineNo := 0
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
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
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- rec:
			sent.Add(1)
		}
		if limit > 0 && int(sent.Load()) >= limit {
			return nil
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func main() {
	url := flag.String("url", "", "gateway URL")
	csvPath := flag.String("csv", "", "CSV dataset path")
	concurrency := flag.Int("concurrency", 32, "client concurrency")
	limit := flag.Int("limit", 0, "record limit")
	timeout := flag.Duration("timeout", 25*time.Millisecond, "request timeout")
	flag.Parse()
	stats, err := Run(context.Background(), RunOptions{URL: *url, CSVPath: *csvPath, Concurrency: *concurrency, Limit: *limit, Timeout: *timeout})
	if err != nil {
		fmt.Fprintf(os.Stderr, "loadtest error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("total=%d success=%d errors=%d qps=%.2f p50=%s p95=%s p99=%s\n", stats.Total, stats.Success, stats.Errors, stats.QPS, stats.P50, stats.P95, stats.P99)
}

func buildStats(total, success, errorsN int64, start time.Time, latencies []time.Duration) Stats {
	elapsed := time.Since(start)
	if elapsed <= 0 {
		elapsed = time.Nanosecond
	}
	used := latencies[:0]
	for _, v := range latencies {
		if v > 0 {
			used = append(used, v)
		}
	}
	sort.Slice(used, func(i, j int) bool { return used[i] < used[j] })
	return Stats{Total: total, Success: success, Errors: errorsN, QPS: float64(success) / elapsed.Seconds(), P50: percentile(used, 50), P95: percentile(used, 95), P99: percentile(used, 99)}
}

func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := (len(sorted)*p + 99) / 100
	if idx <= 0 {
		idx = 1
	}
	if idx > len(sorted) {
		idx = len(sorted)
	}
	return sorted[idx-1]
}

func splitCSVLine(line string) []string {
	var fields []string
	start := 0
	for i := 0; i <= len(line); i++ {
		if i == len(line) || line[i] == ',' {
			fields = append(fields, line[start:i])
			start = i + 1
		}
	}
	return fields
}

func looksLikeHeader(line string) bool {
	lower := strings.ToLower(line)
	return strings.Contains(lower, "userid") || strings.Contains(lower, "reviewerid") || strings.Contains(lower, "unixreviewtime")
}

func parseHeader(line string) map[string]int {
	fields := splitCSVLine(line)
	h := make(map[string]int, len(fields))
	for i, f := range fields {
		h[strings.ToLower(strings.TrimSpace(f))] = i
	}
	return h
}

func headerIndex(header map[string]int, names ...string) (int, bool) {
	for _, name := range names {
		if idx, ok := header[name]; ok {
			return idx, true
		}
	}
	return 0, false
}

func appendUUID(dst []byte, seed string) []byte {
	var h uint64 = 1469598103934665603
	for i := 0; i < len(seed); i++ {
		h ^= uint64(seed[i])
		h *= 1099511628211
	}
	var hex [32]byte
	for i := 31; i >= 0; i-- {
		hex[i] = "0123456789abcdef"[h&15]
		h = h*1099511628211 + uint64(i+1)
	}
	for i := 0; i < 32; i++ {
		if i == 8 || i == 12 || i == 16 || i == 20 {
			dst = append(dst, '-')
		}
		dst = append(dst, hex[i])
	}
	return dst
}

func brandFromItem(item string) string {
	if item == "" {
		return "unknown"
	}
	if len(item) <= 16 {
		return item
	}
	return item[:16]
}

func appendEscaped(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\', '"':
			dst = append(dst, '\\', s[i])
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			if s[i] >= 0x20 {
				dst = append(dst, s[i])
			}
		}
	}
	return dst
}
