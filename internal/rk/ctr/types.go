package ctr

import "context"

// Candidate represents a single recommendation candidate from any recall source.
type Candidate struct {
	ItemID   string
	Score    float32  // original recall score
	Source   string   // "cf", "content", "profile"
	Features []float32 // sparse feature vector for CTR model
}

// RankedItem is a candidate after CTR scoring.
type RankedItem struct {
	ItemID   string
	Score    float32  // original recall score
	CTRScore float32  // CTR prediction from AFM model
	Source   string
}

// Merger merges candidates from multiple recall sources.
type Merger interface {
	Merge(candidates ...[]Candidate) []Candidate
}

// Ranker scores candidates using a CTR model and returns top-K.
type Ranker interface {
	Rank(ctx context.Context, candidates []Candidate, topK int) ([]RankedItem, error)
}
