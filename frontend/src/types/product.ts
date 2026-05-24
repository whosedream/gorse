export interface ProductItem {
  item_id: string
  title: string
  category: string
  price: number
  image_url: string
  score?: number
  rank?: number
}

export type RerankStatus = 'idle' | 'streaming' | 'inferring' | 'reranking' | 'complete'
