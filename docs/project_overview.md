# Go-Rec：电商 AI 智能导购双轨重排系统 · 2026.05

## 项目概述

设计并实现基于快慢轨架构的在线智能重排引擎。同步快轨在 **25ms P99** 窗口内完成 FSM 流式协议解析、特征缓存 I/O、并
发打分、防漂移融合与 TopK 多样性打散；异步慢轨通过 Kafka 泵送行为事件，依托纯 Go 原生 DAG 工作流调度 DeepSeek LLM 与 BGE-M3 Embedding 完成意图解构与向量化，最终将 1024 维意图特征回写 Redis 热缓存，使快轨在下一周期自动提权命中品类。

## 核心架构与组件

### 同步快轨

- **`/pkg/fsm`**：手写有限状态机，逐字节扫描 JSON 字节流，零反射解析 `session_id`、`version_stamp`、`slots` 等关键
  字段并就地写入固定数组结构体。基准测试 `0 allocs/op`。
- **`/pkg/pool`**：自适应有界协程池，泛型对象池与 4KB 字节数组池，支持非阻塞背压、60% 水位自动扩容、空闲缩容与关闭 drain。
- **`/internal/rk/scorer`**：内积打分 + 滑动窗口多样性打散引擎，热路径 `0 allocs/op`。支持 `RankParallel` 通过协程池
  并发打分并归并 TopK。
- **`/internal/rk/anti_drift`**：核查-反馈-修正-复核闭环。慢轨输出携带基线时间戳，写入时对比快轨最新版本，超阈值
  触发 alpha 衰减融合，旧版本被 Lua 脚本拦截。
- **`/api`**：HTTP 网关，`pkg/pool` 获取临时上下文，字节流直入 `pkg/fsm`，串联 cache I/O → anti_drift coordinator → scorer `RankParallel`。限流 429 非阻塞，2ms Redis 读超时静默降级。

### 异步慢轨

- **`/pkg/mq`**：`kafka-go` Writer（Async=true，Key=SessionID 保序）与 Kafka Reader WindowConsumer，按 `SessionID` 微批聚合、At-Least-Once 提交。
- **`/pkg/agent`**：纯 Go 原生 DAG 工作流引擎。节点分为 LLM 神经节点（Prompt 注入 XML 模板 + `<think>` 剥离 + Markdown fence 清洗 + 正则提取 JSON）和 Go 符号节点。114>Embedding 节点调用 BGE-M3，失败自动 fallback 本地 1024 维兜底向量。StateSync 尾节点通过 Redis Lua 原子写入在线特征缓存。
- **`/internal/slow_track`**：DeepSeek 客户端，CoT 推理支持，429/5xx 重试熔断器，`context.WithoutCancel` 隔离上游取消。
- **`/pkg/cache`**：Redis Hash `HMGET` 读取 intent、`encoding/binary.LittleEndian` 4096 字节原地解码 (`0 allocs/op`)、Lua 原子版本 CAS 写入、24h TTL。

## 快慢轨双闭环与状态同步

快轨请求经 FSM 解析后异步推入 Kafka。慢轨 daemon 消费聚合后的 batch，启动 DAG 工作流完成 LLM 推理 → Embedding 向量化 → Redis 回写。下一轮快轨请求通过 Redis Intent Reader（2ms 熔断窗口）读取该意图向量并注入 scorer 权重调整，实现“一次推理、持续提权”的闭环。

## 基础设施

- Go 1.24 · Kafka KRaft (Apache Kafka 3.7) · Redis 7 · Docker Compose · DeepSeek · BGE-M3
- DuckDB（`go-duckdb` 已接入，可用于离线特征分析、评价指标计算与行为日志 SQL 审计）
- 前端控制台：Vue 3 + Vuetify 3 + Vite，含流式慢轨日志面板、SSE 打字机模拟与商品卡片 FLIP 重排动画

## 真实数据集压测

| 数据集 | 规模 | 成功率 | QPS | P99 |
|---|---|---|---|---|
| Taobao UserBehavior.csv | 1 亿条 (3.67GB) | 98.0% | 25,175 | 13.2ms |
| Amazon Ratings Electronics | 782 万条 (319MB) | 99.3% | 26,779 | 9.3ms |

## 技术栈

| 分层 | 选型 |
|---|---|
| 语言 | Go |
| 快轨解析 | 手写 FSM · 零反射 |
| 内存复用 | `sync.Pool` · 泛型 `MemoryPool[T]` |
| 协程并发 | 自研有界池 · 非阻塞背压 |
| 消息队列 | Kafka（`segmentio/kafka-go`）|
| 在线缓存 | Redis（`go-redis/v9`）|
| 基座大模型 | DeepSeek V4 Pro · CoT 推理 |
| 向量化 | BGE-M3（SiliconFlow / OpenAI 兼容 API）|
| DuckDB | `marcboeker/go-duckdb`（分析 SQL 引擎）|
| 前端 | Vue 3 · Vuetify 3 · Vite |
