import type { ProductItem } from '../types/product'

const baseProducts: ProductItem[] = [
  {
    item_id: 'sku-cat-001',
    title: '鲜肉冻干猫咪全价主粮 1.8kg',
    category: '猫咪用品',
    price: 169,
    image_url: 'https://images.unsplash.com/photo-1589924691995-400dc9ecc119?auto=format&fit=crop&w=720&q=80',
    score: 0.74,
  },
  {
    item_id: 'sku-phone-002',
    title: '旗舰影像手机 Pro 512GB',
    category: '数码电子',
    price: 5299,
    image_url: 'https://images.unsplash.com/photo-1598327105666-5b89351aff97?auto=format&fit=crop&w=720&q=80',
    score: 0.91,
  },
  {
    item_id: 'sku-cat-003',
    title: '无尘混合豆腐猫砂 6L x 4',
    category: '猫咪用品',
    price: 89,
    image_url: 'https://images.unsplash.com/photo-1574144611937-0df059b5ef3e?auto=format&fit=crop&w=720&q=80',
    score: 0.69,
  },
  {
    item_id: 'sku-shoe-004',
    title: '城市通勤缓震跑鞋',
    category: '运动户外',
    price: 459,
    image_url: 'https://images.unsplash.com/photo-1542291026-7eec264c27ff?auto=format&fit=crop&w=720&q=80',
    score: 0.86,
  },
  {
    item_id: 'sku-cat-005',
    title: '幼猫高蛋白营养罐头 12 罐',
    category: '猫咪用品',
    price: 118,
    image_url: 'https://images.unsplash.com/photo-1518791841217-8f162f1e1131?auto=format&fit=crop&w=720&q=80',
    score: 0.66,
  },
  {
    item_id: 'sku-coffee-006',
    title: '冷萃精品咖啡豆礼盒',
    category: '食品饮料',
    price: 139,
    image_url: 'https://images.unsplash.com/photo-1447933601403-0c6688de566e?auto=format&fit=crop&w=720&q=80',
    score: 0.83,
  },
  {
    item_id: 'sku-cat-007',
    title: '智能循环猫咪饮水机 2.5L',
    category: '猫咪用品',
    price: 229,
    image_url: 'https://images.unsplash.com/photo-1592194996308-7b43878e84a6?auto=format&fit=crop&w=720&q=80',
    score: 0.71,
  },
  {
    item_id: 'sku-bag-008',
    title: '轻量商务双肩包 18L',
    category: '箱包服饰',
    price: 329,
    image_url: 'https://images.unsplash.com/photo-1553062407-98eeb64c6a62?auto=format&fit=crop&w=720&q=80',
    score: 0.79,
  },
  {
    item_id: 'sku-cat-009',
    title: '猫抓板太空舱组合玩具',
    category: '猫咪用品',
    price: 159,
    image_url: 'https://images.unsplash.com/photo-1545249390-6bdfa286032f?auto=format&fit=crop&w=720&q=80',
    score: 0.63,
  },
  {
    item_id: 'sku-lamp-010',
    title: '桌面护眼氛围台灯',
    category: '家居生活',
    price: 269,
    image_url: 'https://images.unsplash.com/photo-1507473885765-e6ed057f782c?auto=format&fit=crop&w=720&q=80',
    score: 0.81,
  },
  {
    item_id: 'sku-cat-011',
    title: '低敏无谷成猫主粮 5kg',
    category: '猫咪用品',
    price: 299,
    image_url: 'https://images.unsplash.com/photo-1555685812-4b943f1cb0eb?auto=format&fit=crop&w=720&q=80',
    score: 0.68,
  },
  {
    item_id: 'sku-watch-012',
    title: 'AI 健康运动手表',
    category: '智能穿戴',
    price: 1299,
    image_url: 'https://images.unsplash.com/photo-1523275335684-37898b6baf30?auto=format&fit=crop&w=720&q=80',
    score: 0.88,
  },
]

const rerankedIds = [
  'sku-cat-001',
  'sku-cat-011',
  'sku-cat-007',
  'sku-cat-003',
  'sku-cat-005',
  'sku-cat-009',
  'sku-phone-002',
  'sku-watch-012',
  'sku-coffee-006',
  'sku-lamp-010',
  'sku-shoe-004',
  'sku-bag-008',
]

function withRanks(products: ProductItem[]): ProductItem[] {
  return products.map((product, index) => ({ ...product, rank: index + 1 }))
}

export function getProductCatalog(): ProductItem[] {
  return withRanks(baseProducts)
}

export function getBaselineProducts(): ProductItem[] {
  return getProductCatalog()
}

export function getCatIntentRerankedProducts(): ProductItem[] {
  const byId = new Map(baseProducts.map((product) => [product.item_id, product]))
  return withRanks(
    rerankedIds.map((id, index) => {
      const product = byId.get(id)
      if (!product) {
        throw new Error(`Missing mock product: ${id}`)
      }

      const isCatIntent = product.category === '猫咪用品'
      return {
        ...product,
        score: isCatIntent ? Number((0.98 - index * 0.025).toFixed(2)) : product.score,
      }
    }),
  )
}
