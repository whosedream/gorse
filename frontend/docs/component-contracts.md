# Component Contracts

## ProductCard

`ProductCard` receives a single product object with the following locked fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `item_id` | `string` | yes | Stable product identifier used as Vue key and rerank identity. |
| `title` | `string` | yes | Product display title. |
| `category` | `string` | yes | Primary ecommerce category. |
| `price` | `number` | yes | Product price in CNY. |
| `image_url` | `string` | yes | Product image URL; UI must provide a stable fallback placeholder on load error. |
| `score` | `number` | no | Ranking/intent relevance score. |
| `rank` | `number` | no | Current visible ranking position. |

TypeScript shape:

```ts
export interface ProductItem {
  item_id: string
  title: string
  category: string
  price: number
  image_url: string
  score?: number
  rank?: number
}
```
