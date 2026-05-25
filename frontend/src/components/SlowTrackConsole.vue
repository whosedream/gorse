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
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import type { RerankStatus } from '../types/product'

const props = defineProps<{
  runId: number
  sessionId: string
  status: RerankStatus
  collapsed: boolean
}>()

const emit = defineEmits<{
  complete: []
  toggle: []
  phase: [phase: number]
}>()

const phases = ['行为泵送', 'LLM意图解构', '向量化降维', 'Redis状态同步回写']
const lines = ref<string[]>([])
const activePhase = ref(-1)
const consoleBody = ref<HTMLElement | null>(null)
let eventSource: InstanceType<typeof window.EventSource> | undefined

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

function phaseForLine(line: string): number {
  if (line.includes('[LLM推理开始]') || line.includes('[反思触发]') || line.includes('[LLM推理完成]')) return 1
  if (line.includes('[向量生成]')) return 2
  if (line.includes('[Redis写入成功]')) return 3
  return 0
}

async function scrollToBottom(): Promise<void> {
  await nextTick()
  if (consoleBody.value) {
    consoleBody.value.scrollTop = consoleBody.value.scrollHeight
  }
}

function closeStream(): void {
  if (eventSource) {
    eventSource.close()
    eventSource = undefined
  }
}

function openStream(): void {
  closeStream()
  if (!props.sessionId) return
  eventSource = new window.EventSource(`http://127.0.0.1:18080/stream?session_id=${encodeURIComponent(props.sessionId)}`)
  eventSource.onmessage = (event) => {
    const line = event.data
    lines.value.push(line)
    const phase = phaseForLine(line)
    if (phase > activePhase.value) {
      activePhase.value = phase
      emit('phase', phase)
    }
    if (line.includes('[Redis写入成功]')) {
      emit('complete')
    }
    void scrollToBottom()
  }
}

watch(
  () => props.runId,
  (runId) => {
    if (runId > 0) {
      lines.value = []
      activePhase.value = 0
      emit('phase', 0)
    }
  },
)

watch(
  () => props.sessionId,
  () => openStream(),
)

onMounted(() => openStream())

onBeforeUnmount(() => {
  closeStream()
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
