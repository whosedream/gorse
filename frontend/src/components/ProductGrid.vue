<template>
  <div>
    <div class="grid-header">
      <div>
        <p class="eyebrow">Fast Track Ranking Surface</p>
        <h2>电商商品瀑布流重排</h2>
      </div>
      <v-chip :color="rerankMode === 'ai-hit' ? 'accent' : 'primary'" variant="tonal">
        {{ rerankMode === 'ai-hit' ? 'AI 深度意图重排 (Hit)' : rerankMode === 'fallback' ? '基线基础召回 (Fallback)' : 'Set A: 基线排序' }}
      </v-chip>
    </div>

    <TransitionGroup
      v-auto-animate
      name="product"
      tag="div"
      class="product-grid"
    >
      <ProductCard
        v-for="product in products"
        :key="product.item_id"
        :product="product"
        :selected="selectedId === product.item_id"
        :busy="busy"
        @select="emit('select', product)"
      />
    </TransitionGroup>
  </div>
</template>

<script setup lang="ts">
import { vAutoAnimate } from '@formkit/auto-animate/vue'
import ProductCard from './ProductCard.vue'
import type { ProductItem, RerankMode } from '../types/product'

defineProps<{
  products: ProductItem[]
  selectedId: string | null
  busy: boolean
  rerankMode: RerankMode
}>()

const emit = defineEmits<{
  select: [product: ProductItem]
}>()

void vAutoAnimate
</script>

<style scoped>
.grid-header {
  display: flex;
  gap: 16px;
  align-items: end;
  justify-content: space-between;
  margin-bottom: 18px;
}

.eyebrow {
  margin: 0 0 4px;
  font-family: 'JetBrains Mono', 'Consolas', monospace;
  font-size: 12px;
  color: #66e3ff;
  letter-spacing: 0.18em;
  text-transform: uppercase;
}

h2 {
  margin: 0;
  font-size: clamp(24px, 3vw, 38px);
  letter-spacing: -0.04em;
}

.product-grid {
  position: relative;
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
  gap: 18px;
  align-items: start;
}

.product-grid :deep(.product-card:nth-child(3n + 1)) {
  margin-top: 0;
}

.product-grid :deep(.product-card:nth-child(3n + 2)) {
  margin-top: 22px;
}

.product-grid :deep(.product-card:nth-child(3n)) {
  margin-top: 10px;
}

.product-move,
.product-enter-active,
.product-leave-active {
  transition:
    transform 560ms cubic-bezier(0.22, 1, 0.36, 1),
    opacity 340ms ease;
}

.product-enter-from,
.product-leave-to {
  opacity: 0;
  transform: translateY(18px) scale(0.97);
}

.product-leave-active {
  position: absolute;
}

@media (max-width: 760px) {
  .grid-header {
    align-items: flex-start;
    flex-direction: column;
  }

  .product-grid {
    grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
  }

  .product-grid :deep(.product-card) {
    margin-top: 0 !important;
  }
}
</style>
