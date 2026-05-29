# Go-Rec — 电商 AI 双轨重排系统

Go-Rec 是一个 Go 实现的电商智能导购重排系统。核心目标是：**在线快轨严守 P99 ≤ 25ms** 的同时，把**多轮行为理解、LLM 意图推断、Embedding 向量化和 Redis 热特征回写**全部异步化到慢轨进程，让大模型的深度推理能力为下一次点击提供真实可观测的意图增强排序。

前后端已经端到端打通：浏览器点击商品后，左侧瀑布流瞬间 (<25ms) 重排，右侧开发者控制台流式输出 Agent 进程的 LLM 思维链 / 分类权重 / 向量生成 / Redis 写入实时事件。

## 当前状态

### 全链路已联通

- **行为事件泵送**：快轨 `/rerank` 处理完后用裸 goroutine（非有界池）将 `mq.Event{session_id, user_id, item_id, category_id, ...}` `PUBLISH` 到 Redis Pub/Sub `behavior:events` 频道，发布严禁阻塞 25ms 红线。
- **Kafka + Redis 双发**：`FanoutProducer` 并行（`sync.WaitGroup`）向 Kafka 与 Redis 发布事件，Kafka 异步写入失败不阻塞 Redis 路径。
- **Agent 慢轨守护进程**：独立的 `cmd/agent` 进程通过 `RedisEventSource` + `WindowConsumer` 订阅 `behavior:events`，每条事件实例化 `neural_intent → embedding → state_sync` DAG。
- **SSE 实时日志通道**：DAG 各节点通过 `RedisLogPublisher` 推送到 `sse_log:<session_id>`，`GET /stream?session_id=...` 端点订阅 Redis Pub/Sub 并以 SSE 格式回推前端。
- **完整 LLM 推理可观测**：成功路径下，SSE 推送的不只是阶段标记，而是 LLM 原始 JSON 输出（含 `category_weights`）+ 排序后的人类可读分类权重，配合反思触发标记。
- **前端真实联调**：`frontend/src/App.vue` 通过 `EventSource` 接入 `/stream`，点击商品 → POST `/rerank`（携带 `item_id` 与 `user_id`）→ 快轨重排可视化 + 慢轨打字机式日志，无任何 mock。

### 快轨主路径

- **手写 FSM JSON 解析**：单字节状态机，对象池复用 `requestState`/`parseState`，0 allocs/op。
- **A/B 分流**：基于 SessionID 的 FNV-1a 稳定分桶（`0~49 → baseline`，`50~99 → neuro_rerank`）。
- **意图三层降级**：
  1. Redis 慢轨预热向量（`IntentReader` L1 + Ristretto，命中路径 0 allocs）
  2. DuckDB intent search + Scorer 并行余弦打分
  3. 本地 MemoryClient + Coordinator 默认意图兜底
  4. Catalog 静态序列彻底兜底
- **过载防御**：`MaxInFlight` 信号量 + 25ms 上下文超时；池满显式抛 `ErrOverloaded`，不排队不阻塞。

### 慢轨主路径

- **PromptBuilder**：行为日志压缩 → XML 风格 `<system_directive>` / `<behavior_log>` / `<reasoning_protocol>` / `<output_schema>` 注入。
- **反思机制**：旧意图与最新点击品类背离时注入 `<reflection_instruction>`，强制 JSON 契约不变，输出 reflection_active 元数据。
- **JSON 清洗**：`CleanModelJSON` 用 `IndexByte('{')` + `LastIndexByte('}')` 截取，鲁棒处理 DeepSeek 的 `<think>` 噪声与 ```` ```json ```` 包裹。
- **意图载荷校验**：要求 `session_id != ""` 且 `len(category_weights) > 0`，否则触发重试 / 模拟降级。
- **远程 Embedding**：通过 SiliconFlow BAAI/bge-m3 生成 1024 维向量，Embedding 节点失败时本地确定性回退。
- **StateSync**：Lua 原子脚本写入 Redis，TTL 5 分钟。

## 核心架构

```
                              ┌──────────────────────────────────────────┐
   浏览器点击商品               │  快轨 (P99 ≤ 25ms)                       │
   POST /rerank ─────────────► │  FSM 解析 → A/B 分桶 →                   │
                              │  Redis L1/L2 Intent → DuckDB/Scorer →    │
   ◄───── JSON 响应 ─────────── │  (fallback + intent_hit)                  │
                              └────────────┬─────────────────────────────┘
                                           │
                                           │ 裸 goroutine 异步发布
                                           ▼
                              ┌──────────────────────────────────────────┐
                              │  FanoutProducer (并行)                    │
                              │   ├─► Kafka  (rk_behavior_log)           │
                              │   └─► Redis  Pub/Sub  behavior:events    │
                              └────────────┬─────────────────────────────┘
                                           │
                                           ▼
                              ┌──────────────────────────────────────────┐
   GET /stream                │  慢轨 Agent (150-800ms LLM + Embedding)   │
   SSE EventSource ◄──────────┤  WindowConsumer → DAG:                   │
                              │    ① NeuralIntentNode (DeepSeek + 反思)   │
                              │    ② EmbeddingNode  (BGE-M3 1024dim)     │
                              │    ③ StateSyncNode  (Redis Lua 原子写入)  │
                              │  每节点 publish 到 sse_log:<session_id>   │
                              └──────────────────────────────────────────┘
                                           │
                                           ▼ (下次 rerank 即可命中)
                                  Redis intent vector 热预热
```

## 重排逻辑全景

### 流量分桶

```
SessionID → FNV-1a hash % 100
  ├── 0~49  → baseline    (50%)
  └── 50~99 → neuro_rerank (50%)
```

### 路径 A：baseline（无意图，纯搜索）

```
POST /rerank → 提取 category → DuckDB.SearchBaseline()
  ├── 成功      → fallback=false, intent_hit=false
  └── 失败/缺失 → Catalog 兜底 → fallback=true,  intent_hit=false
```

### 路径 B：neuro_rerank（意图驱动，三层降级）

```
POST /rerank
  │
  ├── ① Redis 读取慢轨预热的 intent vector（2ms timeout）
  │    │
  │    ├── 命中  → DuckDB.SearchWithIntent(vector, category)
  │    │         → Scorer.Rank() → fallback=false, intent_hit=true   ← 最优
  │    └── 未命中/超时 → ②
  │
  ├── ② Cache Fallback（本地内存打分）
  │    │
  │    ├── Coordinator.Get(sessionID) 查本地特征记录
  │    │   ├── 有记录 → 用 IntentVector 打分
  │    │   └── 无记录 → fillDefaultIntent(category, brand) 默认向量
  │    └── MemoryClient.MGet → Scorer.RankParallel → 返回
  │
  └── ③ Catalog 兜底（路径全断时）
```

### 打分引擎

```
所有候选商品 × intent vector → 余弦相似度
  → selectTopK (取 Top 20)
  → fillDiverseResults (滑动窗口 5, 同 category ≤ 3, 同 brand ≤ 3)
```

### 响应字段

| 字段 | 含义 |
|---|---|
| `fallback` | 是否走了 catalog / 默认向量 等降级路径 |
| `intent_hit` | 是否命中 Redis 中由慢轨预热的 LLM 意图向量 |

## SSE 事件流（开发者控制台）

```
[LLM推理开始] 慢轨意图解构启动
[反思触发]   检测到感知漂移反思上下文        ← 仅当 metadata.reflection_active=true
{"session_id":"...","category_weights":{"咖啡茶饮":1.0}}   ← LLM 原始 JSON
[意图解构]   分类权重: 咖啡茶饮:1.00          ← 格式化后
[LLM推理完成] 意图解构完成 耗时=4.5s
[向量生成]   