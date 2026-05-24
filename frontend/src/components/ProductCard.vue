<template>
  <v-card
    class="product-card"
    :class="{
      'product-card--selected': selected,
      'product-card--cat': product.category === '猫咪用品',
      'product-card--busy': busy,
    }"
    :elevation="selected ? 18 : 8"
    ripple
    @click="emit('select', product)"
  >
    <div class="rank-badge">#{{ product.rank ?? '--' }}</div>

    <div class="image-shell">
      <v-img
        v-if="!imageFailed"
        :src="product.image_url"
        :alt="product.title"
        cover
        height="178"
        @error="imageFailed = true"
      >
        <template #placeholder>
          <v-skeleton-loader type="image" height="178" />
        </template>
      </v-img>
      <div v-else class="image-fallback">
        <span>{{ categoryGlyph }}</span>
        <strong>Go-Rec</strong>
        <small>image fallback</small>
      </div>
    </div>

    <v-card-text class="pa-4">
      <div class="d-flex align-center justify-space-between mb-2 ga-2">
        <v-chip
          size="small"
          :color="product.category === '猫咪用品' ? 'accent' : 'primary'"
          variant="tonal"
        >
          {{ product.category }}
        </v-chip>
        <span class="score">score {{ formattedScore }}</span>
      </div>

      <h3 class="title">{{ product.title }}</h3>

      <div class="tag-row">
        <v-chip size="x-small" variant="outlined" color="secondary">快轨候选</v-chip>
        <v-chip
          v-if="product.category === '猫咪用品'"
          size="x-small"
          variant="outlined"
          color="accent"
        >
          猫咪意图
        </v-chip>
      </div>

      <div class="footer-row">
        <span class="price">¥{{ product.price.toLocaleString('zh-CN') }}</span>
        <span class="latency">P99 &lt; 25ms</span>
      </div>
    </v-card-text>
  </v-card>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import type { ProductItem } from '../types/product'

const props = defineProps<{
  product: ProductItem
  selected: boolean
  busy: boolean
}>()

const emit = defineEmits<{
  select: [product: ProductItem]
}>()

const imageFailed = ref(false)

const formattedScore = computed(() => (props.product.score ?? 0).toFixed(2))
const categoryGlyph = computed(() => (props.product.category === '猫咪用品' ? 'CAT' : 'AI'))

watch(
  () => props.product.image_url,
  () => {
    imageFailed.value = false
  },
)
</script>

<style scoped>
.product-card {
  position: relative;
  overflow: hidden;
  border: 1px solid rgba(102, 227, 255, 0.16);
  background: linear-gradient(160deg, rgba(18, 31, 52, 0.96), rgba(9, 16, 30, 0.98));
  transition:
    transform 220ms ease,
    box-shadow 220ms ease,
    border-color 220ms ease;
  cursor: pointer;
}

.product-card::before {
  position: absolute;
  inset: 0;
  pointer-events: none;
  content: '';
  background: radial-gradient(circle at 18% 0%, rgba(102, 227, 255, 0.22), transparent 34%);
  opacity: 0.72;
}

.product-card:hover {
  transform: translateY(-8px);
  border-color: rgba(102, 227, 255, 0.55);
  box-shadow: 0 24px 58px rgba(0, 0, 0, 0.42), 0 0 34px rgba(102, 227, 255, 0.18);
}

.product-card--selected {
  border-color: rgba(25, 245, 168, 0.86);
  box-shadow: 0 0 0 1px rgba(25, 245, 168, 0.42), 0 0 38px rgba(25, 245, 168, 0.2);
}

.product-card--cat {
  background: linear-gradient(160deg, rgba(17, 44, 45, 0.98), rgba(10, 18, 31, 0.98));
}

.product-card--busy {
  cursor: wait;
}

.rank-badge {
  position: absolute;
  z-index: 2;
  top: 12px;
  left: 12px;
  padding: 5px 10px;
  font-family: 'JetBrains Mono', 'Consolas', monospace;
  font-size: 12px;
  font-weight: 800;
  color: #06111f;
  background: linear-gradient(135deg, #66e3ff, #19f5a8);
  border-radius: 999px;
  box-shadow: 0 0 24px rgba(25, 245, 168, 0.35);
}

.image-shell {
  height: 178px;
  background: rgba(255, 255, 255, 0.04);
}

.image-fallback {
  display: grid;
  height: 178px;
  place-items: center;
  align-content: center;
  gap: 6px;
  color: rgba(255, 255, 255, 0.78);
  background:
    linear-gradient(135deg, rgba(102, 227, 255, 0.18), transparent),
    repeating-linear-gradient(45deg, rgba(255, 255, 255, 0.04) 0 8px, transparent 8px 16px),
    #111b2d;
}

.image-fallback span {
  font-family: 'JetBrains Mono', 'Consolas', monospace;
  font-size: 34px;
  font-weight: 900;
  letter-spacing: 0.18em;
}

.image-fallback small,
.score,
.latency {
  font-family: 'JetBrains Mono', 'Consolas', monospace;
  font-size: 11px;
  color: rgba(255, 255, 255, 0.58);
}

.title {
  min-height: 48px;
  margin: 0 0 12px;
  font-size: 16px;
  line-height: 1.45;
  color: rgba(255, 255, 255, 0.94);
}

.tag-row,
.footer-row {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: center;
}

.footer-row {
  justify-content: space-between;
  margin-top: 16px;
}

.price {
  font-size: 20px;
  font-weight: 900;
  color: #fff;
}
</style>
