<template>
  <v-card class="console-card" elevation="14">
    <div class="console-toolbar">
      <div>
        <p class="eyebrow">Developer Mode</p>
        <h3>慢轨 LLM 推理链路</h3>
      </div>
      <v-btn
        size="small"
        variant="outlined"
        color="accent"
        @click="emit('toggle')"
      >
        {{ collapsed ? '展开' : '收起' }}
      </v-btn>
    </div>

    <v-expand-transition>
      <div v-show="!collapsed">
        <div class="status-row">
          <v-chip size="small" :color="statusColor" variant="tonal">{{ statusText }}</v-chip>
          <span>{{ activePhaseLabel }}</span>
        </div>

        <div ref="consoleBody" class="console-body">
          <div v-if="lines.length === 0" class="placeholder">
            $ 等待用户点击商品，慢轨将异步泵送行为事件...
          </div>
          <div v-for="(line, index) in lines" :key="`${index}-${line}`" class="log-line">
            <span class="prompt">go-rec@slow-track</span>
            <span>{{ line }}</span>
          </div>
          <div v-if="streamingText" class="log-line active">
            <span class="prompt">go-rec@slow-track</span>
            <span>{{ streamingText }}</span><span class="cursor" />
          </div>
        </div>

        <div class="phase-grid">
          <div
            v-for="(phase, index) in phases"
            :key="phase"
            class="phase-pill"
            :class="{ 'phase-pill--active': activePhase >= index, 'phase-pill--done': status === 'complete' }"
          >
            {{ phase }}
          </div>
        </div>
      </div>
    </v-expand-transition>
  </v-card>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, ref, watch } from 'vue'
import type { RerankStatus } from '../types/product'

const props = defineProps<{
  runId: number
  status: RerankStatus
  collapsed: boolean
}>()

const emit = defineEmits<{
  complete: []
  toggle: []
  phase: [phase: number]
}>()

const phases = ['行为泵送', 'LLM意图解构', '向量化降维', 'Redis状态同步回写']
const script = [
  '[行为泵送] 发现连击行为：用户连续命中猫粮、猫砂、饮水机候选卡片...',
  '[行为泵送] 抽取多轮会话上下文：最近 7 次曝光 / 3 次点击 / 1 次收藏...',
  '[LLM意图解构] 正在唤醒 DeepSeek 意图推理链路，构造慢轨 Prompt DAG...',
  '[LLM意图解构] 意图锁定：猫咪用品 / 主粮 / 高转化偏好 / 价格敏感度中等...',
  '[向量化降维] 开始降维生成在线特征向量：cat_food=0.97, litter=0.86, toy=0.61...',
  '[向量化降维] 计算防感知漂移：delta=2.4%，低于 5% 安全阈值...',
  '[Redis状态同步回写] 写入快轨热特征缓存 user:intent:demo:cat@v2...',
  '[Redis状态同步回写] 触发重排：提升命中猫咪意图商品权重，准备 Set B...',
]

const lines = ref<string[]>([])
const streamingText = ref('')
const activePhase = ref(-1)
const consoleBody = ref<HTMLElement | null>(null)
let intervalId: number | undefined
let lineIndex = 0
let charIndex = 0

const statusText = computed(() => {
  const mapping: Record<RerankStatus, string> = {
    idle: 'idle',
    streaming: 'streaming',
    inferring: 'inferring',
    reranking: 'reranking',
    complete: 'complete',
  }
  return mapping[props.status]
})

const statusColor = computed(() => {
  if (props.status === 'complete') return 'success'
  if (props.status === 'reranking') return 'warning'
  if (props.status === 'idle') return 'primary'
  return 'accent'
})

const activePhaseLabel = computed(() => {
  if (activePhase.value < 0) return '等待事件'
  return phases[Math.min(activePhase.value, phases.length - 1)]
})

function phaseForLine(index: number): number {
  return Math.min(Math.floor(index / 2), phases.length - 1)
}

function clearTimer(): void {
  if (intervalId !== undefined) {
    window.clearInterval(intervalId)
    intervalId = undefined
  }
}

async function scrollToBottom(): Promise<void> {
  await nextTick()
  if (consoleBody.value) {
    consoleBody.value.scrollTop = consoleBody.value.scrollHeight
  }
}

function startStreaming(): void {
  clearTimer()
  lines.value = []
  streamingText.value = ''
  activePhase.value = 0
  emit('phase', 0)
  lineIndex = 0
  charIndex = 0

  intervalId = window.setInterval(() => {
    const currentLine = script[lineIndex]
    if (!currentLine) {
      clearTimer()
      streamingText.value = ''
      emit('complete')
      return
    }

    activePhase.value = phaseForLine(lineIndex)
    emit('phase', activePhase.value)
    streamingText.value = currentLine.slice(0, charIndex + 1)
    charIndex += 2

    if (charIndex >= currentLine.length) {
      lines.value.push(currentLine)
      streamingText.value = ''
      lineIndex += 1
      charIndex = 0
    }

    void scrollToBottom()
  }, 28)
}

watch(
  () => props.runId,
  (runId) => {
    if (runId > 0) {
      startStreaming()
    }
  },
)

onBeforeUnmount(() => {
  clearTimer()
})
</script>

<style scoped>
.console-card {
  position: sticky;
  top: 18px;
  overflow: hidden;
  border: 1px solid rgba(25, 245, 168, 0.22);
  background:
    linear-gradient(rgba(255, 255, 255, 0.025) 50%, transparent 50%) 0 0 / 100% 4px,
    radial-gradient(circle at top right, rgba(25, 245, 168, 0.16), transparent 38%),
    #050a12;
  box-shadow: 0 0 48px rgba(25, 245, 168, 0.12), inset 0 0 32px rgba(25, 245, 168, 0.04);
}

.console-toolbar {
  display: flex;
  gap: 12px;
  align-items: center;
  justify-content: space-between;
  padding: 18px;
  border-bottom: 1px solid rgba(25, 245, 168, 0.16);
}

.eyebrow {
  margin: 0 0 4px;
  font-family: 'JetBrains Mono', 'Consolas', monospace;
  font-size: 11px;
  color: #19f5a8;
  letter-spacing: 0.16em;
  text-transform: uppercase;
}

h3 {
  margin: 0;
  font-size: 18px;
}

.status-row {
  display: flex;
  gap: 10px;
  align-items: center;
  padding: 14px 18px 0;
  font-family: 'JetBrains Mono', 'Consolas', monospace;
  font-size: 12px;
  color: rgba(255, 255, 255, 0.7);
}

.console-body {
  height: 430px;
  margin: 14px 14px 0;
  padding: 16px;
  overflow: auto;
  font-family: 'JetBrains Mono', 'Consolas', monospace;
  font-size: 12px;
  line-height: 1.7;
  color: #d7ffe9;
  background: rgba(0, 0, 0, 0.42);
  border: 1px solid rgba(25, 245, 168, 0.14);
  border-radius: 18px;
}

.placeholder {
  color: rgba(215, 255, 233, 0.46);
}

.log-line {
  margin-bottom: 8px;
  text-shadow: 0 0 12px rgba(25, 245, 168, 0.22);
}

.log-line.active {
  color: #ffffff;
}

.prompt {
  display: block;
  color: #19f5a8;
}

.cursor {
  display: inline-block;
  width: 7px;
  height: 14px;
  margin-left: 3px;
  vertical-align: -2px;
  background: #19f5a8;
  animation: blink 0.9s steps(2, start) infinite;
}

.phase-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 8px;
  padding: 14px;
}

.phase-pill {
  padding: 10px;
  font-size: 12px;
  color: rgba(255, 255, 255, 0.48);
  text-align: center;
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 12px;
}

.phase-pill--active {
  color: #19f5a8;
  border-color: rgba(25, 245, 168, 0.45);
  box-shadow: inset 0 0 18px rgba(25, 245, 168, 0.08);
}

.phase-pill--done {
  background: rgba(25, 245, 168, 0.06);
}

@keyframes blink {
  50% {
    opacity: 0;
  }
}
</style>
