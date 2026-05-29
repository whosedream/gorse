package cf

import "context"

// Candidate represents a recalled item with its relevance score and source tag.
type Candidate struct {
	ItemID string
	Score  float32
	Source string // "cf", "content", "profile"
}

// Recaller is the common interface for all recall paths.
type Recaller interface {
	Recall(ctx context.Context, userID string, topK int) ([]Candidate, error)
}
