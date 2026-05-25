<template>
  <v-app>
    <main class="app-shell">
      <section class="hero-panel">
        <div>
          <p class="eyebrow">Go-Rec Dual Track Reranking</p>
          <h1>快轨不降速，慢轨懂意图</h1>
          <p class="subtitle">
            点击任意商品后，慢轨模拟 DeepSeek 推理并回写热特征缓存；左侧快轨调用真实 /rerank API，商品网格按后端结果自动重排。
          </p>
        </div>
        <div class="metric-row">
          <v-card class="metric-card" variant="tonal">
            <strong>25ms</strong><span>快轨 P99 红线</span>
          </v-card>
          <v-card class="metric-card" variant="tonal">
            <strong>{{ activePhaseLabel }}</strong><span>慢轨阶段</span>
          </v-card>
          <v-card class="metric-card" variant="tonal">
            <strong>{{ isReranked ? 'Set B' : 'Set A' }}</strong><span>当前排序</span>
          </v-card>
        </div>
      </section>

      <v-row class="content-row" align="start">
        <v-col cols="12" lg="8" xl="9">
          <ProductGrid
            :products="products"
            :selected-id="selectedId"
            :busy="isBusy"
            :rerank-mode="rerankMode"
            @select="handleProductSelect"
          />
        </v-col>

        <v-col cols="12" lg="4" xl="3">
          <SlowTrackConsole
            :run-id="runId"
            :session-id="sessionId"
            :status="status"
            :collapsed="consoleCollapsed"
            @complete="handleConsoleComplete"
            @toggle="consoleCollapsed = !consoleCollapsed"
            @phase="activePhase = $event"
          />
        </v-col>
      </v-row>
    </main>
  </v-app>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import ProductGrid from './components/ProductGrid.vue'
import SlowTrackConsole from './components/SlowTrackConsole.vue'
import { getProductCatalog } from './mock/products'
import type { ProductItem, RerankMode, RerankResponse, RerankStatus } from './types/product'

const phaseLabels = ['行为泵送', 'LLM意图解构', '向量化降维', 'Redis状态同步回写']

const catalog = getProductCatalog()
const products = ref<ProductItem[]>(catalog)
const selectedId = ref<string | null>(null)
const status = ref<RerankStatus>('idle')
const runId = ref(0)
const rerankMode = ref<RerankMode>('baseline')
const consoleCollapsed = ref(false)
const activePhase = ref(-1)
const storedSessionId = window.localStorage.getItem('go-rec-session-id')
const sessionId = storedSessionId && experimentBucket(storedSessionId) >= 50 ? storedSessionId : createExperimentSessionId()

window.localStorage.setItem('go-rec-session-id', sessionId)

const isBusy = computed(() => ['streaming', 'inferring', 'reranking'].includes(status.value))
const isReranked = computed(() => rerankMode.value !== 'baseline')
const activePhaseLabel = computed(() => (activePhase.value >= 0 ? phaseLabels[activePhase.value] : 'Idle'))

function experimentBucket(value: string): number {
  let h = 2166136261
  for (let i = 0; i < value.length; i += 1) {
    h ^= value.charCodeAt(i)
    h = Math.imul(h, 16777619) >>> 0
  }
  return h % 100
}

function createExperimentSessionId(): string {
  for (let i = 0; i < 32; i += 1) {
    const candidate = window.crypto.randomUUID()
    if (experimentBucket(candidate) >= 50) {
      return candidate
    }
  }
  return '00000000-0000-4000-8000-000000000003'
}

function productForResult(result: RerankResponse['results'][number], index: number): ProductItem {
  const backendId = String(result.id)
  const metadata = catalog.find((product) => product.item_id === backendId) ?? catalog[index % catalog.length]
  return {
    ...metadata,
    item_id: backendId,
    title: metadata ? `${metadata.title} · #${backendId}` : `Backend Result #${backendId}`,
    score: result.score,
    rank: index + 1,
  }
}

function applyRerankResults(response: RerankResponse): void {
  if (response.results.length === 0) {
    return
  }

  const orderedProducts = response.results.map((result, index) => productForResult(result, index))

  if (orderedProducts.length > 0) {
    products.value = orderedProducts
  }
  rerankMode.value = response.fallback || !response.intent_hit ? 'fallback' : 'ai-hit'
}

async function handleProductSelect(product: ProductItem): Promise<void> {
  if (isBusy.value) {
    return
  }

  selectedId.value = product.item_id
  status.value = 'streaming'
  rerankMode.value = 'baseline'
  products.value = catalog
  activePhase.value = 0
  runId.value += 1

  try {
    status.value = 'reranking'
    const response = await window.fetch('http://127.0.0.1:18080/rerank', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        session_id: sessionId,
        version_stamp: Date.now(),
        item_id: product.item_id,
        slots: {
          category: product.category,
          brand: product.brand ?? 'default',
        },
      }),
    })

    if (!response.ok) {
      throw new Error(`Rerank request failed: ${response.status}`)
    }

    applyRerankResults((await response.json()) as RerankResponse)
  } catch (error) {
    console.error('Failed to fetch rerank results', error)
    status.value = 'idle'
    activePhase.value = -1
  }
}

function handleConsoleComplete(): void {
  status.value = 'complete'
  activePhase.value = 3
}
</script>

<style scoped>
.app-shell {
  min-height: 100vh;
  padding: clamp(18px, 3vw, 42px);
  color: #fff;
  background:
    radial-gradient(circle at top left, rgba(102, 227, 255, 0.24), transparent 34%),
    radial-gradient(circle at 76% 12%, rgba(155, 124, 255, 0.22), transparent 30%),
    linear-gradient(135deg, #07101d 0%, #0c1424 50%, #070a12 100%);
}

.hero-panel {
  display: flex;
  gap: 24px;
  align-items: end;
  justify-content: space-between;
  margin-bottom: 26px;
  padding: clamp(18px, 3vw, 30px);
  border: 1px solid rgba(102, 227, 255, 0.16);
  border-radius: 28px;
  background: rgba(9, 17, 31, 0.7);
  box-shadow: 0 24px 90px rgba(0, 0, 0, 0.24);
  backdrop-filter: blur(18px);
}

.eyebrow {
  margin: 0 0 8px;
  font-family: 'JetBrains Mono', 'Consolas', monospace;
  font-size: 12px;
  color: #66e3ff;
  letter-spacing: 0.18em;
  text-transform: uppercase;
}

h1 {
  max-width: 760px;
  margin: 0;
  font-size: clamp(34px, 6vw, 74px);
  line-height: 0.96;
  letter-spacing: -0.07em;
}

.subtitle {
  max-width: 820px;
  margin: 18px 0 0;
  color: rgba(255, 255, 255, 0.68);
  font-size: 16px;
  line-height: 1.7;
}

.metric-row {
  display: grid;
  min-width: 280px;
  grid-template-columns: repeat(3, minmax(88px, 1fr));
  gap: 10px;
}

.metric-card {
  padding: 14px;
  border: 1px solid rgba(255, 255, 255, 0.08);
  background: rgba(255, 255, 255, 0.05) !important;
}

.metric-card strong,
.metric-card span {
  display: block;
}

.metric-card strong {
  font-size: 20px;
  color: #19f5a8;
}

.metric-card span {
  margin-top: 4px;
  font-size: 11px;
  color: rgba(255, 255, 255, 0.58);
}

.content-row {
  position: relative;
}

@media (max-width: 1264px) {
  .hero-panel {
    align-items: stretch;
    flex-direction: column;
  }

  .metric-row {
    min-width: 0;
  }
}

@media (max-width: 680px) {
  .metric-row {
    grid-template-columns: 1fr;
  }
}
</style>
