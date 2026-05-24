# /pkg/pool 四维验收闭环验证报告

## 环境刷新
PowerShell 每次执行前均刷新：
`$machinePath = [Environment]::GetEnvironmentVariable('Path', 'Machine'); $userPath = [Environment]::GetEnvironmentVariable('Path', 'User'); $env:Path = "$machinePath;$userPath"; $env:CGO_ENABLED = [Environment]::GetEnvironmentVariable('CGO_ENABLED', 'User')`

## 代码变更
- `D:\CodeField\Go-Rec\pkg\pool\memory_test.go`：将非确定性 `time.Nanosecond` timeout 测试改为确定性 canceled/deadline 上下文表格用例。
- `D:\CodeField\Go-Rec\pkg\pool\pool_bench_test.go`：新增对象池热复用、字节池热复用、过载背压、并发 P99 压测 benchmark。
- 生产代码无修改。

## 命令日志

### 0. 红灯复现：修复前基线测试
Command: `go test -count=1 ./pkg/...`
Exit code: 1

```text
--- FAIL: TestMemoryPoolWithResetsAndPreventsCrossRequestPollution (0.00s)
    --- FAIL: TestMemoryPoolWithResetsAndPreventsCrossRequestPollution/context_timeout_cancels_before_acquisition (0.00s)
        memory_test.go:105: expected context timeout error
FAIL
FAIL	go-rec/pkg/pool	0.019s
FAIL
```

根因：`context.WithTimeout(..., time.Nanosecond)` 在 Windows 调度下可能尚未过期即被 `With` 首个非阻塞 select 观察，测试非确定性，不是生产逻辑缺陷。

### 1. 功能测试
Command: `go test -count=1 ./pkg/...`
Exit code: 0

```text
ok  	go-rec/pkg/pool	0.020s
```

### 2. 竞态检测
Command: `go test -race -count=1 ./pkg/...`
Exit code: 0

```text
ok  	go-rec/pkg/pool	1.059s
```

### 3. 逃逸分析
Command: `go build -gcflags="-m -l" ./pkg/...`
Exit code: 0

```text
# go-rec/pkg/pool
pkg\pool/goroutine.go:142:7: p does not escape
pkg\pool/goroutine.go:187:14: leaking param: task
pkg\pool/goroutine.go:176:7: p does not escape
pkg\pool/goroutine.go:146:7: leaking param: p
pkg\pool/goroutine.go:147:8: func literal does not escape
pkg\pool/goroutine.go:136:7: leaking param: p
pkg\pool/goroutine.go:51:7: &GoroutinePool{...} escapes to heap
pkg\pool/goroutine.go:125:7: leaking param: p
pkg\pool/goroutine.go:67:7: leaking param: p
pkg\pool/goroutine.go:67:32: leaking param: ctx
pkg\pool/goroutine.go:67:53: leaking param: fn
pkg\pool/goroutine.go:96:7: p does not escape
pkg\pool/goroutine.go:101:7: p does not escape
pkg\pool/goroutine.go:106:7: leaking param: p
pkg\pool/goroutine.go:106:34: leaking param: ctx
pkg\pool/goroutine.go:107:20: func literal does not escape
pkg\pool/goroutine.go:112:6: func literal escapes to heap
pkg\pool/goroutine.go:196:43: leaking param: p
pkg\pool/goroutine.go:196:84: leaking param content: branches
pkg\pool/goroutine.go:214:29: leaking param: taskCtx
pkg\pool/goroutine.go:196:22: moved to heap: ctx
pkg\pool/goroutine.go:200:6: moved to heap: cancel
pkg\pool/goroutine.go:208:6: moved to heap: once
pkg\pool/goroutine.go:214:24: func literal escapes to heap
pkg\pool/goroutine.go:217:13: func literal does not escape
pkg\pool/memory.go:73:3: moved to heap: buf
pkg\pool/memory.go:71:8: &ByteBufferPool{...} escapes to heap
pkg\pool/memory.go:72:16: func literal escapes to heap
pkg\pool/memory.go:73:14: make([]byte, 0, minCap) escapes to heap
pkg\pool/memory.go:117:16: buf does not escape
pkg\pool/memory.go:80:7: leaking param: p
pkg\pool/memory.go:80:31: leaking param: ctx
pkg\pool/memory.go:80:62: fn does not escape
pkg\pool/memory.go:93:13: make([]byte, 0, need) escapes to heap
pkg\pool/memory.go:98:8: func literal does not escape
pkg\pool/memory.go:100:14: make([]byte, 0, p.minCap) escapes to heap
```

判断：热路径 callback 不逃逸：`pkg\pool/memory.go:80:62: fn does not escape`、`pkg\pool/memory.go:98:8: func literal does not escape`；buffer 仅在创建/扩容/超长回收时逃逸，非热复用路径。goroutine pool 的 ctx/fn/task 逃逸符合跨 goroutine 队列语义，不属于对象复用热路径。

### 4. 全量 benchmark
Command: `go test '-bench=.' '-benchmem' './pkg/...'`
Exit code: 0

```text
goos: windows
goarch: amd64
pkg: go-rec/pkg/pool
cpu: AMD Ryzen 5 5600 6-Core Processor              
BenchmarkMemoryPoolHotReuse-12                 	51238038	        22.42 ns/op	       0 B/op	       0 allocs/op
BenchmarkByteBufferPoolHotReuse-12             	50616038	        23.36 ns/op	       0 B/op	       0 allocs/op
BenchmarkGoroutinePoolSubmitBackpressure-12    	35853109	        33.89 ns/op	       0 B/op	       0 allocs/op
BenchmarkGoroutinePoolStressP99-12             	 1000000	      1096 ns/op	         0 p50_ms	         0 p95_ms	         0.5146 p99_ms	      80 B/op	       1 allocs/op
PASS
ok  	go-rec/pkg/pool	4.766s
```

备注：PowerShell 5.1 下未加引号执行 `go test -bench=. -benchmem ./pkg/...` 曾被解析为测试当前目录并失败：`no Go files in D:\CodeField\Go-Rec`。等价 quoted 形式通过并产出 benchmark。

### 5. 独立极限压测 benchmark
Command: `go test ./pkg/pool -run '^$' -bench '.' -benchmem`
Exit code: 0

```text
goos: windows
goarch: amd64
pkg: go-rec/pkg/pool
cpu: AMD Ryzen 5 5600 6-Core Processor              
BenchmarkMemoryPoolHotReuse-12                 	53204931	        22.67 ns/op	       0 B/op	       0 allocs/op
BenchmarkByteBufferPoolHotReuse-12             	51623116	        23.85 ns/op	       0 B/op	       0 allocs/op
BenchmarkGoroutinePoolSubmitBackpressure-12    	35492142	        33.99 ns/op	       0 B/op	       0 allocs/op
BenchmarkGoroutinePoolStressP99-12             	 1000000	      1100 ns/op	         0 p50_ms	         0 p95_ms	         0.5202 p99_ms	      80 B/op	       1 allocs/op
PASS
ok  	go-rec/pkg/pool	4.856s
```

压测方法：`BenchmarkGoroutinePoolStressP99` 使用固定 64 workers、队列 32768、提交侧并发门限 128，采集 1,000,000 个提交至执行完成延迟样本，排序后上报 P50/P95/P99。P50/P95 因 ns 级结果按 ms 输出为 0，P99=0.5202ms，小于 5ms。

### 6. 自主编译纠错闭环
Command: `go build ./...; if ($?) { go test -v -race ./... }`
Exit code: 0

```text
=== RUN   TestGoroutinePoolExecutionBackpressureTimeoutAndScaling
=== PAUSE TestGoroutinePoolExecutionBackpressureTimeoutAndScaling
=== RUN   TestParallelExtractFastFailsAndCancelsOtherBranches
=== PAUSE TestParallelExtractFastFailsAndCancelsOtherBranches
=== RUN   TestMemoryPoolWithResetsAndPreventsCrossRequestPollution
=== PAUSE TestMemoryPoolWithResetsAndPreventsCrossRequestPollution
=== RUN   TestByteBufferPoolClearsSensitiveDataAndHandlesLongInput
=== PAUSE TestByteBufferPoolClearsSensitiveDataAndHandlesLongInput
=== RUN   TestMemoryPoolsHandleConcurrentGetPutSurge
=== PAUSE TestMemoryPoolsHandleConcurrentGetPutSurge
=== CONT  TestParallelExtractFastFailsAndCancelsOtherBranches
=== CONT  TestByteBufferPoolClearsSensitiveDataAndHandlesLongInput
=== CONT  TestMemoryPoolWithResetsAndPreventsCrossRequestPollution
=== RUN   TestParallelExtractFastFailsAndCancelsOtherBranches/all_branches_succeed
=== RUN   TestByteBufferPoolClearsSensitiveDataAndHandlesLongInput/sensitive_bytes_are_zeroed_before_reuse
=== RUN   TestMemoryPoolWithResetsAndPreventsCrossRequestPollution/object_is_thoroughly_reset_after_reuse
=== CONT  TestGoroutinePoolExecutionBackpressureTimeoutAndScaling
=== CONT  TestMemoryPoolsHandleConcurrentGetPutSurge
=== RUN   TestGoroutinePoolExecutionBackpressureTimeoutAndScaling/nil_task_returns_ErrInvalidTask
=== RUN   TestByteBufferPoolClearsSensitiveDataAndHandlesLongInput/overlong_malicious_input_is_not_retained
=== RUN   TestMemoryPoolWithResetsAndPreventsCrossRequestPollution/empty_input_leaves_zero_state
=== RUN   TestGoroutinePoolExecutionBackpressureTimeoutAndScaling/submit_after_shutdown_returns_ErrPoolClosed
--- PASS: TestByteBufferPoolClearsSensitiveDataAndHandlesLongInput (0.00s)
    --- PASS: TestByteBufferPoolClearsSensitiveDataAndHandlesLongInput/sensitive_bytes_are_zeroed_before_reuse (0.00s)
    --- PASS: TestByteBufferPoolClearsSensitiveDataAndHandlesLongInput/overlong_malicious_input_is_not_retained (0.00s)
=== RUN   TestParallelExtractFastFailsAndCancelsOtherBranches/one_branch_error_cancels_sibling
=== RUN   TestMemoryPoolWithResetsAndPreventsCrossRequestPollution/context_cancellation_rejects_before_acquisition
=== RUN   TestGoroutinePoolExecutionBackpressureTimeoutAndScaling/executes_submitted_tasks_normally
=== RUN   TestParallelExtractFastFailsAndCancelsOtherBranches/timeout_cancels_slow_branch
=== RUN   TestGoroutinePoolExecutionBackpressureTimeoutAndScaling/full_queue_returns_ErrOverloaded_without_blocking
=== RUN   TestMemoryPoolWithResetsAndPreventsCrossRequestPollution/expired_deadline_rejects_before_acquisition
=== RUN   TestGoroutinePoolExecutionBackpressureTimeoutAndScaling/shutdown_drains_queued_tasks_before_workers_exit
--- PASS: TestMemoryPoolWithResetsAndPreventsCrossRequestPollution (0.00s)
    --- PASS: TestMemoryPoolWithResetsAndPreventsCrossRequestPollution/object_is_thoroughly_reset_after_reuse (0.00s)
    --- PASS: TestMemoryPoolWithResetsAndPreventsCrossRequestPollution/empty_input_leaves_zero_state (0.00s)
    --- PASS: TestMemoryPoolWithResetsAndPreventsCrossRequestPollution/context_cancellation_rejects_before_acquisition (0.00s)
    --- PASS: TestMemoryPoolWithResetsAndPreventsCrossRequestPollution/expired_deadline_rejects_before_acquisition (0.00s)
=== RUN   TestGoroutinePoolExecutionBackpressureTimeoutAndScaling/concurrent_shutdown_and_submit_never_loses_accepted_tasks
=== RUN   TestParallelExtractFastFailsAndCancelsOtherBranches/fast_branch_error_wins_over_later_submit_cancellation
--- PASS: TestMemoryPoolsHandleConcurrentGetPutSurge (0.01s)
=== RUN   TestGoroutinePoolExecutionBackpressureTimeoutAndScaling/idle_shrink_keeps_at_least_min_workers
=== RUN   TestGoroutinePoolExecutionBackpressureTimeoutAndScaling/shutdown_wait_signal_is_allocated_once
=== RUN   TestParallelExtractFastFailsAndCancelsOtherBranches/concurrent_submit_surge_completes_without_lost_tasks
--- PASS: TestParallelExtractFastFailsAndCancelsOtherBranches (0.03s)
    --- PASS: TestParallelExtractFastFailsAndCancelsOtherBranches/all_branches_succeed (0.00s)
    --- PASS: TestParallelExtractFastFailsAndCancelsOtherBranches/one_branch_error_cancels_sibling (0.00s)
    --- PASS: TestParallelExtractFastFailsAndCancelsOtherBranches/timeout_cancels_slow_branch (0.01s)
    --- PASS: TestParallelExtractFastFailsAndCancelsOtherBranches/fast_branch_error_wins_over_later_submit_cancellation (0.02s)
    --- PASS: TestParallelExtractFastFailsAndCancelsOtherBranches/concurrent_submit_surge_completes_without_lost_tasks (0.00s)
=== RUN   TestGoroutinePoolExecutionBackpressureTimeoutAndScaling/canceled_context_is_rejected_before_enqueue
=== RUN   TestGoroutinePoolExecutionBackpressureTimeoutAndScaling/queue_above_sixty_percent_expands_workers
--- PASS: TestGoroutinePoolExecutionBackpressureTimeoutAndScaling (0.03s)
    --- PASS: TestGoroutinePoolExecutionBackpressureTimeoutAndScaling/nil_task_returns_ErrInvalidTask (0.00s)
    --- PASS: TestGoroutinePoolExecutionBackpressureTimeoutAndScaling/submit_after_shutdown_returns_ErrPoolClosed (0.00s)
    --- PASS: TestGoroutinePoolExecutionBackpressureTimeoutAndScaling/executes_submitted_tasks_normally (0.00s)
    --- PASS: TestGoroutinePoolExecutionBackpressureTimeoutAndScaling/full_queue_returns_ErrOverloaded_without_blocking (0.00s)
    --- PASS: TestGoroutinePoolExecutionBackpressureTimeoutAndScaling/shutdown_drains_queued_tasks_before_workers_exit (0.00s)
    --- PASS: TestGoroutinePoolExecutionBackpressureTimeoutAndScaling/concurrent_shutdown_and_submit_never_loses_accepted_tasks (0.02s)
    --- PASS: TestGoroutinePoolExecutionBackpressureTimeoutAndScaling/idle_shrink_keeps_at_least_min_workers (0.00s)
    --- PASS: TestGoroutinePoolExecutionBackpressureTimeoutAndScaling/shutdown_wait_signal_is_allocated_once (0.00s)
    --- PASS: TestGoroutinePoolExecutionBackpressureTimeoutAndScaling/canceled_context_is_rejected_before_enqueue (0.00s)
    --- PASS: TestGoroutinePoolExecutionBackpressureTimeoutAndScaling/queue_above_sixty_percent_expands_workers (0.00s)
PASS
ok  	go-rec/pkg/pool	1.057s
```

## 四维验收结论

1. 静态安全：PASS
   - `go build -gcflags="-m -l" ./pkg/...` 退出码 0。
   - `go test -race -count=1 ./pkg/...` 退出码 0，无竞态报告。
   - 对象/字节池复用热路径 benchmark 均为 0 B/op、0 allocs/op；逃逸分析显示热路径 callback 不逃逸。

2. 功能边界：PASS
   - 表格驱动测试覆盖正常执行、全空状态、超长恶意输入、确定性 canceled/deadline、队列满 ErrOverloaded、并发浪涌、shutdown drain、并发 shutdown/submit。
   - `full_queue_returns_ErrOverloaded_without_blocking` 验证满队列即时背压。
   - deterministic canceled/deadline 测试替换非确定性 ns timeout。

3. 内存性能：PASS
   - `BenchmarkMemoryPoolHotReuse`: 22.42 ns/op, 0 B/op, 0 allocs/op。
   - `BenchmarkByteBufferPoolHotReuse`: 23.36 ns/op, 0 B/op, 0 allocs/op。
   - `BenchmarkGoroutinePoolSubmitBackpressure`: 33.89 ns/op, 0 B/op, 0 allocs/op。

4. 极限压测：PASS
   - `BenchmarkGoroutinePoolStressP99`: 1,000,000 样本，concurrency gate=128，workers=64，queueCap=32768。
   - P50=0ms、P95=0ms、P99=0.5146ms，满足单组件 P99 ≤ 5ms。
   - 压测 benchmark alloc 为 80 B/op、1 allocs/op，来自提交任务闭包跨 goroutine 传递；对象复用热路径单独验证为 0 allocs/op。

## 未达标项和原因

无验收阻断项。

关注项：`GoroutinePool` 队列任务携带 `context.Context` 与 `Task` 函数，逃逸分析报告 `ctx/fn/task` leaking，这是跨 goroutine 队列语义导致，不属于对象池复用热路径；若未来要求 Submit 自身也 0 allocs/op，需要改造为可复用任务描述符/API，不建议在本次验收中扩大生产接口变更。
