---
name: frontend-dashboard-setup
description: Vue rerank dashboard setup choices for future frontend work in this repo
metadata:
  type: project
---

Vue rerank dashboard demo is implemented as an npm-managed Vite + Vue 3 + TypeScript + Vuetify 3 app under the repository frontend area, using contracts-first ProductCard typing and mock deterministic Set A/Set B reranking.

**Why:** The current visualization layer is a standalone demo for the Go-Rec dual-track reranking architecture and must not imply real backend integration.
**How to apply:** Future frontend changes should preserve Vue 3 Composition API, Vuetify 3 syntax, mock-data-driven rerank flow, and clean checker/type/lint verification before reporting completion.
