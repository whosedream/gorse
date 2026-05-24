# /pkg/fsm 四维验收闭环验证报告

## 环境刷新
PowerShell 每次执行前均刷新：
`$machinePath = [Environment]::GetEnvironmentVariable('Path', 'Machine'); $userPath = [Environment]::GetEnvironmentVariable('Path', 'User'); $env:Path = "$machinePath;$userPath"; $env:CGO_ENABLED = [Environment]::GetEnvironmentVariable('CGO_ENABLED', 'User')`

Race 测试额外补充 gcc 路径：
`$env:Path = "$machinePath;$userPath;C:\msys64\ucrt64\bin;D:\redPanda\mingw64\bin"; $env:CGO_ENABLED = '1'`

## 代码变更
- `D:\CodeField\Go-Rec\pkg\fsm\parser.go`：新增手写有限状态机 JSON 字节流解析器，解析 `session_id`、`version_stamp`、`slots.category`、`slots.brand`，并通过 `pkg/pool.MemoryPool` 与 `pkg/pool.ByteBufferPool` 复用解析状态和 scratch buffer。
- `D:\CodeField\Go-Rec\pkg\fsm\parser_test.go`：新增表格驱动测试，覆盖真实 JSON、字段乱序、未知字段跳过、恶意长输入、格式破损、空意图、UUID 校验、时间戳整数/溢出、非法转义、并发复用和跨请求污染防御。
- `D:\CodeField\Go-Rec\pkg\fsm\parser_bench_test.go`：新增解析热路径 0 分配 benchmark。

## 命令日志

### 0. 红灯复现：TDD 初始失败
Command: `go test -run TestParser ./pkg/fsm`
Exit code: 1

```text
--- FAIL: TestParser (0.00s)
    --- FAIL: TestParser/happy_path_parses_core_fields (0.00s)
        parser_test.go:135: SessionIDString = "", want "123e4567-e89b-12d3-a456-426614174000"
FAIL
FAIL    go-rec/pkg/fsm  0.011s
```

根因：测试先于生产 parser 实现落地，初始实现尚不能解析核心字段；后续按 TDD 增量补齐 FSM 解析、严格跳过和 fallback 分支。

### 1. 功能测试
Command: `go test -count=1 ./pkg/fsm`
Exit code: 0

```text
ok  	go-rec/pkg/fsm	0.010s
```

### 2. 竞态检测
Command: `go test -race -count=1 ./pkg/fsm`
Exit code: 0

```text
ok  	go-rec/pkg/fsm	1.026s
```

### 3. 逃逸分析
Command: `go build -gcflags="-m -l" ./pkg/fsm`
Exit code: 0

```text
# go-rec/pkg/fsm
pkg\fsm/parser.go:76:45: input does not escape
pkg\fsm/parser.go:76:59: out does not escape
pkg\fsm/parser.go:96:50: scratch does not escape
pkg\fsm/parser.go:97:37: st does not escape
pkg\fsm/parser.go:121:49: input does not escape
pkg\fsm/parser.go:121:63: out does not escape
pkg\fsm/parser.go:121:83: scratch does not escape
pkg\fsm/parser.go:205:54: input does not escape
pkg\fsm/parser.go:205:68: out does not escape
pkg\fsm/parser.go:205:88: scratch does not escape
pkg\fsm/parser.go:322:38: input does not escape
pkg\fsm/parser.go:322:52: dst does not escape
pkg\fsm/parser.go:322:64: dstLen does not escape
pkg\fsm/parser.go:322:77: scratch does not escape
pkg\fsm/parser.go:51:77: string(r.SessionID[:r.SessionIDLen]) escapes to heap
pkg\fsm/parser.go:52:76: string(r.Category[:r.CategoryLen]) escapes to heap
pkg\fsm/parser.go:53:73: string(r.Brand[:r.BrandLen]) escapes to heap
```

判断：`Parse` 热路径的 `input/out/st/scratch` 均未逃逸；`SessionIDString/CategoryString/BrandString` 的字符串转换仅用于测试与展示，不属于解析热路径。

### 4. 解析热路径 benchmark
Command: `go test ./pkg/fsm -run '^$' -bench BenchmarkParserHotPathNoAlloc -benchmem`
Exit code: 0

```text
goos: windows
goarch: amd64
pkg: go-rec/pkg/fsm
cpu: AMD Ryzen 5 5600 6-Core Processor              
BenchmarkParserHotPathNoAlloc-12    	 2941653	       394.8 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	go-rec/pkg/fsm	1.594s
```

## 四维验收结论

1. 静态安全：PASS
   - `go build -gcflags="-m -l" ./pkg/fsm` 退出码 0。
   - `go test -race -count=1 ./pkg/fsm` 退出码 0，无竞态报告。
   - 代码搜索未发现 `encoding/json`、`reflect`、`map[` 或 `json.` 热路径依赖。

2. 功能边界：PASS
   - 覆盖正常解析、字段乱序、未知 object/array/number/string 严格跳过、必填字段缺失、空 slots、空 category/brand、恶意长输入、超长 value、非法 JSON 转义、UUID 格式、`version_stamp` 小数/指数/溢出、context canceled/deadline、并发浪涌与跨请求污染。
   - 格式破损与超限输入返回错误并置 `Fallback=true`，不会 panic 或卡死。

3. 内存性能：PASS
   - `BenchmarkParserHotPathNoAlloc`: 394.8 ns/op, 0 B/op, 0 allocs/op。
   - `pkg/pool.MemoryPool` 复用 `parseState`，`pkg/pool.ByteBufferPool` 复用转义 key/value scratch buffer。

4. 极限边界：PASS
   - `MaxInputSize=64KiB` 对恶意长输入快速拒绝。
   - 固定数组限制 `session_id` 36 bytes、category/brand 64 bytes；超限返回 `ErrValueTooLarge`。
   - 长 escaped unknown key 超出 scratch 后继续校验后续转义，非法转义不会被吞掉。

## 未达标项和原因

无验收阻断项。

关注项：字符串展示方法会产生堆分配，仅用于测试/展示；生产解析主路径通过固定数组与传入结构体就地写入，benchmark 已验证 0 allocs/op。
