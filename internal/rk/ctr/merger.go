package ctr

// EqualWeightMerger merges candidates from multiple sources with equal weight,
// deduplicates by ItemID (keeping the highest score), and caps at maxCandidates.
type EqualWeightMerger struct {
	MaxCandidates int
}

// NewEqualWeightMerger creates a merger with the given cap. Default is 150.
func NewEqualWeightMerger(maxCandidates int) *EqualWeightMerger {
	if maxCandidates <= 0 {
		maxCandidates = 150
	}
	return &EqualWeightMerger{MaxCandidates: maxCandidates}
}

// Merge combines candidates from multiple sources. Equal-weight means all sources
// contribute equally; dedup keeps the entry with the highest Score per ItemID.
func (m *EqualWeightMerger) Merge(candidates ...[]Candidate) []Candidate {
	total := 0
	for _, c := range candidates {
		total += len(c)
	}
	if total == 0 {
		return nil
	}

	// Dedup map: keep highest score per ItemID.
	best := make(map[string]*Candidate, total)
	// Preserve insertion order for determinism.
	order := make([]string, 0, total)

	for _, batch := range candidates {
		for i := range batch {
			c := batch[i]
			if existing, ok := best[c.ItemID]; ok {
				if c.Score > existing.Score {
					best[c.ItemID] = &c
				}
			} else {
				best[c.ItemID] = &c
				order = append(order, c.ItemID)
			}
		}
	}

	// Sort by score descending (insertion sort is fine for <=150 items).
	result := make([]Candidate, 0, len(order))
	for _, id := range order {
		result = append(result, *best[id])
	}
	// Simple selection sort for small N.
	for i := 0; i < len(result); i++ {
		maxIdx := i
		for j := i + 1; j < len(result); j++ {
			if result[j].Score > result[maxIdx].Score {
				maxIdx = j
			}
		}
		if maxIdx != i {
			result[i], result[maxIdx] = result[maxIdx], result[i]
		}
	}

	// Cap at MaxCandidates.
	if len(result) > m.MaxCandidates {
		result = result[:m.MaxCandidates]
	}
	return result
}
