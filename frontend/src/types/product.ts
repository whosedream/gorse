export interface ProductItem {
  item_id: string
  title: string
  category: string
  brand?: string
  price: number
  image_url: string
  score?: number
  rank?: number
}

export type RerankStatus = 'idle' | 'streaming' | 'inferring' | 'reranking' | 'complete'

export interface RerankResult {
  id: string | number
  score: number
}

export interface RerankResponse {
  fallback: boolean
  intent_hit: boolean
  results: RerankResult[]
}

export type RerankMode = 'baseline' | 'ai-hit' | 'fallback'
