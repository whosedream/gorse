#project_item("Go-Rec — 电商 AI 双轨重排系统 · 2026.05 - 至今")[
  *项目概述：*大模型能深度理解用户意图，但一次推理要几百毫秒，根本塞不进电商推荐那 25ms 的 P99 底线。于是把系统拆成两条轨——快轨在毫秒内完成请求解析、特征拉取和并发打分，死活不超时；慢轨在后台异步跑，等用户点了商品之后才启动 Kafka → DeepSeek 推理 → Embedding 向量化 → 写回 Redis，下轮快轨自动读到新意图，排序跟着变。两条轨互不阻塞。 \
  *手写零分配的 JSON 解析器：*不用 `encoding/json`，不用反射，逐字节状态机直接扫。对象池复用一切临时状态，核心路径 `0 allocs/op`。4KB 字节池 + 泛型池 + 协程 scratch 全覆盖。协程池满了不挂起，直接返回过载错误给上游。 \
  *DuckDB 嵌入向量检索：*用 DuckDB 替代纯内存 mock 作为商品底库，`read_csv_auto` 直接加载 50 条 CSV 商品数据，`array_dot_product(embedding, vec::FLOAT[1024])` 一条 SQL 完成 1024 维相似度打分。跟快轨 scorer 串联，Redis 读到 intent 后不再走固定候选列表，而是先进 DuckDB 筛出最相关的商品再打分。 \
  *纯 Go DAG 工作流，不引入 Python：*自研 DAG 引擎，支持环检测和节点超时。LLM 输出先用正则洗掉 `\`\`\`json` 和 `<think>` 垃圾，再提取 JSON。解析失败带抖动重试，重试耗尽自动 fallback 本地兜底向量。 \
  *真实数据压测跑通 1 亿条：*自研流式 CSV 回放器，逐行读 3.67GB 的 Taobao UserBehavior 数据集，边读边拼 JSON 打 HTTP，统计 QPS 和长尾 P99。单机干到约 25,000 QPS，P99 压在 14ms 以内。Amazon Electronics 782 万条也跑通。 \
  *外部依赖全降级：*Redis 宕了、DeepSeek 熔断、Embedding 超时、Kafka 连不上——快轨全部返回 HTTP 200，走本地基线逻辑。Redis intent 读取有独立 2ms 超时，DuckDB 初始化失败也不阻塞 server 启动。 \
]

---

## 核心架构

```
  ┌────────── 快轨 (P99 ≤ 25ms) ──────────┐
  │  HTTP → FSM 解析 → Cache → AntiDrift →│
  │  Scorer → DuckDB 向量过滤 → 返回重排   │
  └──────────────────┬────────────────────┘
                     │ Kafka Async Producer
  ┌────────── 慢轨 (150-800ms) ──────────┐
  │  Kafka Consume → DAG (LLM+Embedding)  │
  │  → StateSync → Redis Lua 原子写入     │
  └───────────────────────────────────────┘
```

## 关键数字

| 指标 | 数值 |
|---|---|
| FSM JSON 解析 | 395 ns/op · 0 allocs |
| Intent 向量解码 | 257 ns/op · 0 allocs |
| Scorer 打分 | 37 μs/op · 0 allocs |
| 全链路 Pipeline P99 | 1.0 ms |
| Taobao 1亿条压测 P99 | 13.2 ms |
| Taobao 1亿条压测 QPS | 25,175 |
| Amazon 782万条压测 P99 | 9.3 ms |

## 快速开始

```powershell
docker compose up -d kafka
docker run -d --name go-rec-redis -p 6379:6379 redis:7-alpine
go build -tags duckdb_use_lib -o server.exe ./cmd/server && ./server.exe -addr :8080
go run ./test/loadtest -url http://localhost:8080 -csv data/UserBehavior.csv -concurrency 32 -limit 1000
```

## 目录

```
/pkg/fsm     手写 FSM JSON 解析器
/pkg/pool    泛型对象池 · 有界协程池 · 4KB 字节池
/pkg/cache   内存缓存 · Redis Intent 读写
/pkg/mq      Kafka Producer/Consumer
/pkg/agent   DAG 引擎 · Prompt · LLM清洗 · Embedding · StateSync
/pkg/storage DuckDB 向量检索客户端
/internal    快轨 scorer + anti_drift · DeepSeek HTTP 客户端
/api         HTTP 网关
/cmd         快轨 server + 慢轨 agent 入口
/frontend    Vue 3 控制台
/test        流式压测回放器
```
