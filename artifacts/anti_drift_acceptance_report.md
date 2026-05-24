# /internal/rk/anti_drift 四维验收闭环验证报告

## 环境刷新
PowerShell 每次执行前均刷新：
`$machinePath = [Environment]::GetEnvironmentVariable('Path', 'Machine'); $userPath = [Environment]::GetEnvironmentVariable('Path', 'User'); $env:Path = "$machinePath;$userPath"; $env:CGO_ENABLED = [Environment]::GetEnvironmentVariable('CGO_ENABLED', 'User')`

Race 测试额外补充 gcc 路径：
`$env:Path = "$machinePath;$userPath;C:\msys64\ucrt64\bin;D:\redPanda\mingw64\bin"; $env:CGO_ENABLED = '1'`

## 代码变更
- `D:\CodeField\Go-Rec\internal\rk\anti_drift\coordinator.go`：新增异步慢轨防感知漂移协调器，实现“核查-反馈-修正-复核”闭环、漂移窗口计算、alpha 动态融合、慢轨超时/过载 fallback、历史记忆保留、轻量锁台账和 `pkg/pool.GoroutinePool` 调度。
- `D:\CodeField\Go-Rec\internal\rk\anti_drift\coordinator_test.go`：新增表格驱动测试和并发测试，覆盖时间戳对齐、真实毫秒漂移、极端漂移融合、维度不一致融合、慢轨 baseline 单调保护、旧 fast 不降级、默认超时 fallback、pool 过载退避、输入输出深拷贝、并发台账读写。
- `D:\CodeField\Go-Rec\internal\rk\anti_drift\coordinator_bench_test.go`：拆分 `ApplySlow`、`UpdateFast`、`Get`、慢轨 round trip 与过载 fallback benchmark。

## 命令日志

### 0. 红灯复现：TDD 初始失败
Command: `go test -run TestCoordinator ./internal/rk/anti_drift`
Exit code: 1

```text
# go-rec/internal/rk/anti_drift [go-rec/internal/rk/anti_drift.test]
internal\rk\anti_drift\coordinator_test.go:13:44: undefined: IntentFeatureUpdate
internal\rk\anti_drift\coordinator_test.go:19:48: undefined: FeatureRecord
internal\rk\anti_drift\coordinator_test.go:25:44: undefined: SlowTrackService
internal\rk\anti_drift\coordinator_test.go:25:87: undefined: Coordinator
internal\rk\anti_drift\coordinator_test.go:27:12: undefined: NewCoordinator
FAIL	go-rec/internal/rk/anti_drift [build failed]
FAIL
```

后续复审驱动的 RED 还覆盖：新增 `DriftWindowMillis` 前编译失败、`ErrRetryExhausted` 未定义、真实毫秒漂移和旧 baseline 覆盖等边界。

### 1. 功能测试
Command: `go test -count=1 ./internal/rk/anti_drift`
Exit code: 0

```text
ok  	go-rec/internal/rk/anti_drift	0.059s
```

### 2. 竞态检测
Command: `go test -race -count=1 ./internal/rk/... ./pkg/...`
Exit code: 0

```text
ok  	go-rec/internal/rk/anti_drift	1.082s
ok  	go-rec/pkg/fsm	1.032s
ok  	go-rec/pkg/pool	1.082s
```

### 3. 逃逸分析
Command: `go build -gcflags="-m -l" ./internal/rk/anti_drift`
Exit code: 0

```text
# go-rec/internal/rk/anti_drift
internal\rk\anti_drift/coordinator.go:299:20: update does not escape
internal\rk\anti_drift/coordinator.go:303:36: sessionID does not escape
internal\rk\anti_drift/coordinator.go:315:7: c does not escape
internal\rk\anti_drift/coordinator.go:360:33: slow does not escape
internal\rk\anti_drift/coordinator.go:360:39: fast does not escape
internal\rk\anti_drift/coordinator.go:384:33: slow does not escape
internal\rk\anti_drift/coordinator.go:384:39: fast does not escape
internal\rk\anti_drift/coordinator.go:411:20: in does not escape
internal\rk\anti_drift/coordinator.go:420:19: in does not escape
internal\rk\anti_drift/coordinator.go:112:7: &Coordinator{...} escapes to heap
internal\rk\anti_drift/coordinator.go:113:26: make(map[string]FeatureRecord) escapes to heap
internal\rk\anti_drift/coordinator.go:114:26: make(map[string]fastRecord) escapes to heap
internal\rk\anti_drift/coordinator.go:115:26: make(map[string]int64) escapes to heap
internal\rk\anti_drift/coordinator.go:226:32: func literal escapes to heap
```

判断：`driftIndex`、`buildSlowRecord`、`fuseVectors`、`fuseWeights` 的输入参数不逃逸；`Coordinator`、ledger map、slice/map 深拷贝、融合输出和 `Decide` 异步闭包存在堆分配，属于台账生命周期、隔离污染和跨 goroutine 调度的必要成本。

### 4. 分路径 benchmark
Command: `go test ./internal/rk/anti_drift -run '^$' -bench . -benchmem`
Exit code: 0

```text
goos: windows
goarch: amd64
pkg: go-rec/internal/rk/anti_drift
cpu: AMD Ryzen 5 5600 6-Core Processor              
BenchmarkApplySlowFusion-12             	  298324	      4501 ns/op	   21760 B/op	      15 allocs/op
BenchmarkUpdateFast-12                  	 2616399	       448.0 ns/op	     544 B/op	       6 allocs/op
BenchmarkGet-12                         	 4371591	       251.1 ns/op	     272 B/op	       3 allocs/op
BenchmarkDecideSlowTrackRoundTrip-12    	  302233	      4388 ns/op	    2303 B/op	      25 allocs/op
BenchmarkDecideFallbackOverload-12      	  529866	      1963 ns/op	    1452 B/op	      17 allocs/op
PASS
ok  	go-rec/internal/rk/anti_drift	7.906s
```

## 四维验收结论

1. 静态安全：PASS
   - `go build -gcflags="-m -l" ./internal/rk/anti_drift` 退出码 0。
   - `go test -race -count=1 ./internal/rk/... ./pkg/...` 退出码 0，无竞态报告。
   - 使用 `sync.RWMutex` 保护双轨台账；`Decide` 提交 pool 前深拷贝 update，避免 timeout fallback 后慢任务持有调用者可变切片/map。

2. 功能边界：PASS
   - BaselineVersion 强制：空 session、baseline<=0、空向量返回 `ErrInvalidUpdate`。
   - 时间戳对齐：baseline==latest 时接受慢轨特征，不融合。
   - 漂移超阈值：真实毫秒时间戳按 `DriftWindowMillis` 计算，超阈值用 alpha 融合慢轨/快轨向量和权重。
   - 极端漂移：不直接拒绝，仍按 alpha 修正融合。
   - 复核保护：`appliedBaseline` 保证慢轨 baseline 单调；旧 fast snapshot 不降级已有更高版本 record。
   - 降级策略：慢轨错误、超时、pool 过载均 fallback 到蒸馏小模型；已有历史记忆不被 fallback 覆盖。

3. 内存性能：PASS
   - `UpdateFast`: 448.0 ns/op, 544 B/op, 6 allocs/op。
   - `Get`: 251.1 ns/op, 272 B/op, 3 allocs/op。
   - `ApplySlowFusion`: 4501 ns/op, 15 allocs/op，包含 1024 维向量深拷贝与融合输出。
   - `DecideFallbackOverload`: 1963 ns/op，验证 pool 满载时非阻塞 fallback 路径。

4. 极限边界：PASS
   - `ApplySlow` 有界重试 `maxApplySlowRetries=8`，高频 fast 更新下不会无界自旋。
   - `SlowTimeout` 默认 25ms，未显式配置也能毫秒级 fallback。
   - 向量维度不一致按最大维度融合，缺失维度视为 0，避免高维慢轨特征被静默截断。

## 未达标项和原因

无验收阻断项。

关注项：为保证历史记忆完整性、输入输出隔离和异步慢轨安全，`Coordinator` 台账、切片/map 深拷贝、融合输出和 goroutine pool 闭包会产生必要分配。若未来需要进一步压低 `Get/UpdateFast` 分配，可考虑只读视图 API 或调用方提供输出缓冲区，但这会改变当前防污染语义，不建议在本次验收中扩大接口范围。
