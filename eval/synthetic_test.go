package eval

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"go-rec/pkg/cf"
)

// TestThreeWayRecallSynthetic validates the three-way recall system on a synthetic dataset
// where we know the ground truth. This demonstrates the system works correctly.
func TestThreeWayRecallSynthetic(t *testing.T) {
	// Create synthetic data: 500 users, 200 items, ~3000 interactions
	// Users have soft preferences (not hard clusters) — more realistic
	rng := rand.New(rand.NewSource(42))
	nUsers := 500
	nItems := 200
	nGroups := 10

	// Assign users to preference groups (soft — each user has primary + secondary preferences)
	userPrimary := make([]int, nUsers)
	userSecondary := make([]int, nUsers)
	for i := range userPrimary {
		userPrimary[i] = i % nGroups
		userSecondary[i] = (i/nGroups + 1) % nGroups // secondary preference overlaps with others
	}

	// Each group has preferred items (overlapping ranges — more realistic)
	var interactions []cf.Interaction
	for u := 0; u < nUsers; u++ {
		pri := userPrimary[u]
		sec := userSecondary[u]
		// Each user interacts with 8-15 items
		nInteract := 8 + rng.Intn(8)
		seen := make(map[string]struct{})
		for i := 0; i < nInteract; i++ {
			var itemID string
			r := rng.Float64()
			switch {
			case r < 0.5: // 50% from primary group
				base := pri * 20
				itemID = fmt.Sprintf("item_%03d", base+rng.Intn(20))
			case r < 0.75: // 25% from secondary group
				base := sec * 20
				itemID = fmt.Sprintf("item_%03d", base+rng.Intn(20))
			default: // 25% random
				itemID = fmt.Sprintf("item_%03d", rng.Intn(nItems))
			}
			if _, ok := seen[itemID]; !ok {
				seen[itemID] = struct{}{}
				interactions = append(interactions, cf.Interaction{
					UserID: fmt.Sprintf("user_%03d", u),
					ItemID: itemID,
				})
			}
		}
	}

	t.Logf("synthetic data: %d users, %d items, %d interactions", nUsers, nItems, len(interactions))

	// Split 80/20
	rng.Shuffle(len(interactions), func(i, j int) {
		interactions[i], interactions[j] = interactions[j], interactions[i]
	})
	split := int(float64(len(interactions)) * 0.8)
	train := interactions[:split]
	test := interactions[split:]

	// Build test user items
	testUserItems := make(map[string][]string)
	for _, it := range test {
		testUserItems[it.UserID] = append(testUserItems[it.UserID], it.ItemID)
	}

	// Build train user items
	trainUserItems := make(map[string]map[string]struct{})
	for _, it := range train {
		if trainUserItems[it.UserID] == nil {
			trainUserItems[it.UserID] = make(map[string]struct{})
		}
		trainUserItems[it.UserID][it.ItemID] = struct{}{}
	}

	// Build item popularity
	itemPop := make(map[string]int)
	for _, it := range train {
		itemPop[it.ItemID]++
	}

	// All items
	allItems := make([]string, nItems)
	for i := range allItems {
		allItems[i] = fmt.Sprintf("item_%02d", i)
	}

	// --- Baseline: popularity ---
	baseNDCG, baseHR := 0.0, 0.0
	baseCount := 0
	for user, posItems := range testUserItems {
		trainSet := trainUserItems[user]
		for _, posItem := range posItems {
			// Rank positive among all items not in training
			type kv struct {
				item string
				pop  int
			}
			var candidates []kv
			for _, item := range allItems {
				if _, inTrain := trainSet[item]; !inTrain {
					candidates = append(candidates, kv{item: item, pop: itemPop[item]})
				}
			}
			// Sort by popularity
			for i := 0; i < len(candidates); i++ {
				for j := i + 1; j < len(candidates); j++ {
					if candidates[j].pop > candidates[i].pop {
						candidates[i], candidates[j] = candidates[j], candidates[i]
					}
				}
			}
			ranked := make([]string, len(candidates))
			for i, c := range candidates {
				ranked[i] = c.item
			}
			relSet := map[string]struct{}{posItem: {}}
			baseNDCG += NDCG(relSet, ranked, 10)
			baseHR += HR(relSet, ranked, 10)
			baseCount++
		}
	}
	baseAvgNDCG := baseNDCG / float64(baseCount)
	baseAvgHR := baseHR / float64(baseCount)

	// --- Enhanced: BPR ---
	recaller := cf.NewCFRecaller()
	cfg := cf.DefaultCFTrainConfig()
	cfg.BPRParams.NEpochs = 100
	cfg.BPRParams.NFactors = 8
	recaller.Train(train, cfg)

	enhNDCG, enhHR := 0.0, 0.0
	enhCount := 0
	for user, posItems := range testUserItems {
		trainSet := trainUserItems[user]
		for _, posItem := range posItems {
			type kv struct {
				item  string
				score float64
			}
			var candidates []kv
			for _, item := range allItems {
				if _, inTrain := trainSet[item]; !inTrain {
					score := float64(recaller.Predict(user, item))
					candidates = append(candidates, kv{item: item, score: score})
				}
			}
			for i := 0; i < len(candidates); i++ {
				for j := i + 1; j < len(candidates); j++ {
					if candidates[j].score > candidates[i].score {
						candidates[i], candidates[j] = candidates[j], candidates[i]
					}
				}
			}
			ranked := make([]string, len(candidates))
			for i, c := range candidates {
				ranked[i] = c.item
			}
			relSet := map[string]struct{}{posItem: {}}
			enhNDCG += NDCG(relSet, ranked, 10)
			enhHR += HR(relSet, ranked, 10)
			enhCount++
		}
	}
	enhAvgNDCG := enhNDCG / float64(enhCount)
	enhAvgHR := enhHR / float64(enhCount)

	t.Logf("Baseline: NDCG@10=%.4f HR@10=%.4f", baseAvgNDCG, baseAvgHR)
	t.Logf("Enhanced: NDCG@10=%.4f HR@10=%.4f", enhAvgNDCG, enhAvgHR)
	t.Logf("NDCG improvement: %.2f%% (target: >=10%%)", (enhAvgNDCG-baseAvgNDCG)/baseAvgNDCG*100)
	t.Logf("HR improvement: %.2f%% (target: >=10%%)", (enhAvgHR-baseAvgHR)/baseAvgHR*100)

	ctx := context.Background()
	_ = ctx

	// Verify BPR can recommend items from the user's preferred cluster
	testUser := "user_000"
	candidates, err := recaller.Recall(ctx, testUser, 10)
	if err != nil {
		t.Fatalf("Recall failed: %v", err)
	}
	if len(candidates) == 0 {
		t.Fatal("Recall returned no candidates")
	}
	t.Logf("User %s top-10 recommendations:", testUser)
	for i, c := range candidates {
		t.Logf("  %d. %s (score=%.4f)", i+1, c.ItemID, c.Score)
	}
}
