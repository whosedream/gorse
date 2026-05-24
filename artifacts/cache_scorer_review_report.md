# /pkg/cache + /internal/rk/scorer 全链路审查报告

## 审查范围
- `D:\CodeField\Go-Rec\pkg\cache\client.go`
- `D:\CodeField\Go-Rec\pkg\cache\client_test.go`
- `D:\CodeField\Go-Rec\internal\rk\scorer\engine.go`
- `D:\CodeField\Go-Rec\internal\rk\scorer\engine_test.go`
- `D:\CodeField\Go-Rec\internal\rk\scorer\pipeline_bench_test.go`
- `D:\CodeField\Go-Rec\pkg\pool\goroutine.go`
- `D:\CodeField\Go-Rec\pkg\pool\goroutine_test.go`
- 依赖底座：`pkg/fsm`、`internal/rk/anti_drift`

## 审查方法
1. 子智能体 worker 按 TDD 完成初版实现与自检。
2. 第一轮独立复审发现阻断项后回派修复。
3. 主会话对修复后的关键路径执行复测、调试和稳定性验证。
4. 最终 code-reviewer 子审查因 429 配额限制未完成；本报告采用主会话最新命令证据和人工阻断级检查结论。

## 主要复审发现与处置

### 1. MGet 返回前后台 shard goroutine 仍可能写 caller-owned out
- 风险：首错快速返回后，调用方复用 `out` 会与未退出 shard goroutine 产生数据竞争。
- 处置：`MGet` 改为首错 cancel 后等待所有 shard goroutine 退出，再返回错误；`loadShard` 在写每个 `out[i]` 前检查 context。
- 验证：新增 `TestMemoryClientMGetWaitsForShardGoroutinesBeforeReturn`，并修复其非确定性信号通道为带缓冲 channel。

### 2. MGet 每 hit 深拷贝向量导致热路径分配
- 风险：批量拉取 512 候选时每个 cache hit 分配一段向量，违背快轨热路径低分配目标。
- 处置：保留 `MGet` 的防污染深拷贝语义；新增 `MGetInto(ctx, ids, out, vectorBuf, dim)`，由调用方提供向量缓冲区，pipeline benchmark 使用该零分配边界接口。
- 验证：`TestMemoryClientMGetIntoUsesCallerVectorStorage` 与 `TestMemoryClientMGetIntoHotPathNoAlloc` 覆盖 caller buffer 与热路径分配。

### 3. Diversity 在 TopK 截断后执行，无法拉入 TopK 外多样候选
- 风险：例如 TopK=2，候选 `[A:100, A:99, B:98]` 时原实现只能返回 `[A,A]`。
- 处置：`Rank` 先对全量候选排序，再由 `fillDiverseResults` 从 TopK 外拉入满足滑动窗口约束的候选。
- 验证：新增/更新 diversity 用例，覆盖可从 TopK 外拉入候选及极端同类目/同品牌不死循环。

### 4. Rank 排序与打散阶段未响应 context cancel
- 风险：打分阶段检查了 context，但 O(n*k) 选择排序与 diversity 阶段可能越过 deadline 后仍返回成功。
- 处置：`selectTopK` 与 `fillDiverseResults` 增加 context 检查。
- 验证：相关取消测试与 race 测试通过。

### 5. MaxCandidates 不是硬上限
- 风险：超过热路径容量时冷扩容并处理无界候选，导致延迟和内存尖刺。
- 处置：新增 `ErrTooManyCandidates`，`Rank`/`RankParallel` 在候选数超过 `MaxCandidates` 时直接返回错误。
- 验证：`engine_test.go` 覆盖 MaxCandidates 超限错误。

### 6. 全链路 benchmark 未覆盖 anti_drift 融合与并发打分
- 风险：P99 结论不能证明完整 `FSM -> I/O -> Anti-Drift -> 并发打分排序` 路径。
- 处置：benchmark 中先 `UpdateFast`，再 `ApplySlow` 触发 `Fused=true`；循环内解析 FSM、`MGetInto` 拉取特征、`Coordinator.Get` 读取 fused record、`RankParallel` 通过有界 `GoroutinePool` 并发打分排序。
- 验证：benchmark 中检查 `rec.Fused`，并上报 `p99_ms`。

### 7. GoroutinePool 并发扩容可能突破 maxWorkers
- 风险：`Load()+spawnWorker()` 非 CAS，多个 Submit 可能同时看到 `workers < maxWorkers` 并各自扩容。
- 处置：新增 `trySpawnWorker`，通过 CAS 预占 worker 计数后启动 goroutine；idle retire 也使用 CAS 保持不低于 minWorkers。
- 验证：pool scaling 测试重复运行通过。

### 8. ParallelExtract 首错返回前未等待 sibling 退出
- 风险：caller 处理错误并复用共享状态时，未退出 sibling 仍可能写入。
- 处置：`ParallelExtract` 记录已提交分支数，首错 cancel 后等待所有已提交分支发送 done，再返回首错。
- 验证：pool 测试和 race 测试通过。

## 最新命令证据

### 1. 定向单元测试
Command: `go test -count=1 ./pkg/cache ./internal/rk/scorer ./pkg/pool`
Exit code: 0

```text
ok  	go-rec/pkg/cache	0.033s
ok  	go-rec/internal/rk/scorer	0.012s
ok  	go-rec/pkg/pool	0.023s
```

### 2. 全量 race 测试
Command: `go test -race -count=1 ./pkg/... ./internal/rk/...`
Exit code: 0

```text
ok  	go-rec/pkg/cache	1.049s
ok  	go-rec/pkg/fsm	1.029s
ok  	go-rec/pkg/pool	1.078s
ok  	go-rec/internal/rk/anti_drift	1.078s
ok  	go-rec/internal/rk/scorer	1.044s
```

### 3. scorer 与全链路 benchmark
Command: `go test ./internal/rk/scorer -run '^$' -bench . -benchmem`
Exit code: 0

```text
goos: windows
goarch: amd64
pkg: go-rec/internal/rk/scorer
cpu: AMD Ryzen 5 5600 6-Core Processor              
BenchmarkScorerRankHotPathNoAlloc-12    	   31220	     37473 ns/op	       0 B/op	       0 allocs/op
BenchmarkFastTrackPipelineP99-12        	    6441	    182304 ns/op	         1.001 p99_ms	    2765 B/op	      31 allocs/op
PASS
ok  	go-rec/internal/rk/scorer	2.762s
```

结论：scorer 单线程打分排序热路径满足 `0 B/op, 0 allocs/op`；全链路包含 FSM、cache MGetInto、anti_drift fused record、RankParallel 并发打分排序，P99=1.001ms，低于 20ms 红线。

### 4. 逃逸分析
Command: `go build -gcflags="-m -l" ./pkg/cache ./internal/rk/scorer ./pkg/pool`
Exit code: 0

关键片段：
```text
pkg\cache/client.go:231:69: ids does not escape
pkg\cache/client.go:231:82: out does not escape
pkg\cache/client.go:140:17: make([]bool, len(c.shards)) does not escape
pkg\pool/memory.go:80:62: fn does not escape
internal\rk\scorer/engine.go:211:10: a does not escape
internal\rk\scorer/engine.go:211:13: b does not escape
internal\rk\scorer/engine.go:138:44: intent does not escape
internal\rk\scorer/engine.go:138:86: out does not escape
internal\rk\scorer/engine.go:162:33: func literal does not escape
internal\rk\scorer/engine.go:197:38: intent does not escape
internal\rk\scorer/engine.go:197:56: candidates does not escape
internal\rk\scorer/engine.go:197:80: items does not escape
internal\rk\scorer/engine.go:78:118: out does not escape
```

已知分配：
- `pkg/cache.MGet` 普通安全路径仍会深拷贝 vector，保证防污染语义。
- `pkg/cache.MGetInto` 使用调用方 `vectorBuf`，用于 pipeline 热路径降低每 hit 分配。
- `RankParallel` 因 goroutine pool task 闭包存在调度分配；`Rank` 单线程热路径 benchmark 已验证 0 alloc。
- 全链路 benchmark 仍有 31 allocs/op，来自 context、pool 并发调度、anti_drift Get 深拷贝等边界安全成本。

### 5. 全量构建
Command: `go build ./...`
Exit code: 0

## 审查结论

- 规格合规：PASS
  - 已覆盖 cache MGET、I/O 扇出快速失败、内积打分、TopK diversity、pool scratch、anti_drift 融合、RankParallel 并发打分、全链路 P99 benchmark。
- 并发安全：PASS
  - 全量 race 通过；MGet 与 ParallelExtract 均修复为返回前等待相关 goroutine 退出。
- 性能红线：PASS
  - scorer 热路径 0 allocs/op；全链路 P99=1.001ms <20ms。
- 未完成的外部复审：最终 code-reviewer 子智能体因 429 配额限制未返回，已由主会话完成阻断级检查和命令验证。

## 后续建议

1. 若未来要求全链路也降到接近 0 allocs/op，需要为 `anti_drift.Get` 增加调用方输出缓冲接口，替代当前防污染深拷贝返回语义。
2. 若候选规模显著超过 512，应新增更大规模 benchmark，并评估当前 selection TopK 的 O(n*k) 成本是否需要替换为固定容量堆。
3. 若 cache 从内存 mock 切换到 Redis/Aerospike，应保留 `MGetInto` 调用方缓冲语义和“错误返回前等待 I/O goroutine 退出”约束。
