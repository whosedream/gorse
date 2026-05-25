# Go-Rec — 电商 AI 双轨重排系统

Go-Rec 是一个 Go 实现的电商智能导购重排系统。核心目标是在在线快轨保持 P99 ≤ 25ms 的同时，把多轮行为理解、LLM 意图推断、Embedding 向量化和 Redis 热特征回写放到异步慢轨中执行。

## 当前状态

已实现：

- 快轨 HTTP 网关：手写 FSM JSON 解析、对象池复用、有界协程池、过载显式降级。
- 快轨 A/B 分流：基于 SessionID 的 FNV-1a 稳定分桶。
  - `baseline`：跳过 Redis intent，直接走 DuckDB baseline 检索。
  - `neuro_rerank`：Redis intent → DuckDB intent search → 本地缓存 fallback。
- API 可观测字段：`/rerank` 响应包含 `fallback` 和 `intent_hit`。
  - `fallback` 表示重排/底库路径是否使用 fallback。
  - `intent_hit` 表示本次是否命中 Redis/LLM 意图向量。
- 本地 CORS：允许 Vite dev origin `http://127.0.0.1:5173` / `http://localhost:5173` 访问 `/rerank`。
- Redis IntentReader L1：Ristretto + 固定 hot table，命中路径 0 allocs/op，TTL 5 分钟。
- 慢轨 Agent DAG：Kafka batch → PromptBuilder → LLM JSON 清洗 → Embedding → StateSync Redis。
- Reflection 机制：当上一轮意图与当前点击类目明显背离时，在 prompt 中注入反思指令，并保持最终 JSON 契约不变。
- DuckDB 集成：支持带 intent vector 的商品检索；非 CGO/无 DuckDB 环境下有 no-cgo stub，默认构建不阻塞。
- Vue 3 控制台：当前是本地 mock 演示 UI，可展示商品网格和慢轨打字机日志。

已知缺口：

- 前端当前没有真实 `fetch` / SSE / WebSocket 联调，商品重排和慢轨日志仍由 mock 数据与 `setTimeout` / `setInterval` 驱动。
- 慢轨真实可观测流（Kafka → DeepSeek → Redis 的日志流）尚未暴露给前端。
- 如需浏览器真实联调，应新增前端 API client 与后端 SSE/WebSocket 或轮询接口。

## 核心架构

```
  ┌────────── 快轨 (P99 ≤ 25ms) ──────────┐
  │  HTTP → FSM 解析 → A/B 分桶 →          │
  │  Redis L1/L2 Intent → DuckDB/Scorer → │
  │  JSON 响应(fallback + intent_hit)      │
  └──────────────────┬────────────────────┘
                     │ Kafka Async Producer
  ┌────────── 慢轨 (150-800ms) ──────────┐
  │  Kafka Consume → Agent DAG           │
  │  → Reflection Prompt → LLM JSON 清洗 │
  │  → BGE-M3 Embedding → StateSync      │
  │  → Redis Lua 原子写入                │
  └───────────────────────────────────────┘
```

## 关键数字

| 指标 | 数值 |
|---|---:|
| FSM JSON 解析 | 395 ns/op · 0 allocs |
| Intent 向量解码 | 约 240-260 ns/op · 0 allocs |
| RedisIntentReader L1 命中 | 约 40-50 ns/op · 0 allocs |
| Scorer 打分 | 37 μs/op · 0 allocs |
| 全链路 Pipeline P99 | 1.0 ms |
| Taobao 1亿条压测 P99 | 13.2 ms |
| Taobao 1亿条压测 QPS | 25,175 |
| Amazon 782万条压测 P99 | 9.3 ms |

## 快速开始

### 后端

```powershell
# 可选：启动依赖
docker compose up -d kafka
docker run -d --name go-rec-redis -p 6379:6379 redis:7-alpine

# 默认非 DuckDB CGO 构建，Redis 默认地址为 127.0.0.1:6379
$env:REDIS_ADDR = "127.0.0.1:6379"
go run ./cmd/server -addr 127.0.0.1:8080
```

### API 冒烟

```powershell
$body = '{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1710000000123,"slots":{"category":"phone","brand":"acme"}}'
Invoke-WebRequest -UseBasicParsing -Method Post -Uri "http://127.0.0.1:8080/rerank" -ContentType "application/json" -Body $body
```

响应会包含：

```json
{
  "session_id": "...",
  "fallback": false,
  "intent_hit": false,
  "results": []
}
```

### 前端

```powershell
npm --prefix frontend install
npm --prefix frontend run dev -- --host 127.0.0.1 --port 5173
```

打开：`http://127.0.0.1:5173/`

## 测试

```powershell
# Go 全量非 CGO 测试
go test ./...

# 快轨 / 慢轨重点包
go test ./api ./pkg/agent ./pkg/cache ./cmd/server ./cmd/agent

# 前端类型检查与构建
npm --prefix frontend run typecheck
npm --prefix frontend run build
```

## 目录

```
/pkg/fsm      手写 FSM JSON 解析器
/pkg/pool     泛型对象池 · 有界协程池 · 4KB 字节池
/pkg/cache    内存缓存 · Redis Intent 读写 · Ristretto L1
/pkg/mq       Kafka Producer/Consumer
/pkg/agent    DAG 引擎 · Reflection Prompt · LLM 清洗 · Embedding · StateSync
/pkg/storage  DuckDB 向量检索客户端
/internal     快轨 scorer + anti_drift · DeepSeek HTTP 客户端
/api          HTTP 网关 · A/B 分流 · CORS · intent_hit 响应
/cmd          快轨 server + 慢轨 agent 入口
/frontend     Vue 3 控制台，目前为 mock 演示 UI
/test         流式压测回放器
```

## 本地配置

`.env` 不应提交；`.env.example` 保留可提交模板。

关键默认值：

```env
REDIS_ADDR=127.0.0.1:6379
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=rk_behavior_log
KAFKA_GROUP_ID=rk_slow_track
```
