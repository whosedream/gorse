package eval

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"

	"go-rec/pkg/cf"
)

// LoadResult holds the loaded dataset split into user-item interactions and auxiliary maps.
type LoadResult struct {
	Interactions []cf.Interaction
	UserItems    map[string]map[string]struct{} // userID -> set of itemIDs
	ItemUsers    map[string]map[string]struct{} // itemID -> set of userIDs
	UserIDs      []string
	ItemIDs      []string
}

// LoadCSV reads an Amazon Reviews CSV and returns interactions.
// Columns: reviewerID, asin, category, unixReviewTime.
// If maxSamples > 0, randomly samples that many records.
func LoadCSV(path string, maxSamples int) (*LoadResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open csv: %w", err)
	}
	defer f.Close()

	var records [][3]string // [userID, itemID, _]
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	lineNo := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		lineNo++
		if line == "" {
			continue
		}
		// Skip header
		if lineNo == 1 {
			lower := strings.ToLower(line)
			if strings.Contains(lower, "reviewerid") || strings.Contains(lower, "userid") {
				continue
			}
		}

		fields := strings.SplitN(line, ",", 4)
		if len(fields) < 3 {
			continue
		}
		userID := strings.TrimSpace(fields[0])
		itemID := strings.TrimSpace(fields[1])
		if userID == "" || itemID == "" {
			continue
		}
		records = append(records, [3]string{userID, itemID, ""})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan csv: %w", err)
	}

	// Sample if needed
	if maxSamples > 0 && len(records) > maxSamples {
		rng := rand.New(rand.NewSource(42))
		rng.Shuffle(len(records), func(i, j int) {
			records[i], records[j] = records[j], records[i]
		})
		records = records[:maxSamples]
	}

	// Deduplicate: keep unique (user, item) pairs
	seen := make(map[[2]string]struct{}, len(records))
	userItems := make(map[string]map[string]struct{})
	itemUsers := make(map[string]map[string]struct{})
	userSet := make(map[string]struct{})
	itemSet := make(map[string]struct{})

	var interactions []cf.Interaction
	for _, r := range records {
		key := [2]string{r[0], r[1]}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		interactions = append(interactions, cf.Interaction{UserID: r[0], ItemID: r[1]})
		userSet[r[0]] = struct{}{}
		itemSet[r[1]] = struct{}{}

		if userItems[r[0]] == nil {
			userItems[r[0]] = make(map[string]struct{})
		}
		userItems[r[0]][r[1]] = struct{}{}

		if itemUsers[r[1]] == nil {
			itemUsers[r[1]] = make(map[string]struct{})
		}
		itemUsers[r[1]][r[0]] = struct{}{}
	}

	userIDs := make([]string, 0, len(userSet))
	for uid := range userSet {
		userIDs = append(userIDs, uid)
	}
	itemIDs := make([]string, 0, len(itemSet))
	for iid := range itemSet {
		itemIDs = append(itemIDs, iid)
	}

	return &LoadResult{
		Interactions: interactions,
		UserItems:    userItems,
		ItemUsers:    itemUsers,
		UserIDs:      userIDs,
		ItemIDs:      itemIDs,
	}, nil
}

// SplitTrainTest splits interactions into train and test sets for a given fold.
// The split is per-user: for each user, 80% of their interactions go to train, 20% to test.
func SplitTrainTest(data *LoadResult, fold int, trainRatio float64) (train, test []cf.Interaction) {
	rng := rand.New(rand.NewSource(int64(fold*1000 + 42)))

	// Group interactions by user
	byUser := make(map[string][]cf.Interaction)
	for _, it := range data.Interactions {
		byUser[it.UserID] = append(byUser[it.UserID], it)
	}

	for _, userInteractions := range byUser {
		// Shuffle this user's interactions
		rng.Shuffle(len(userInteractions), func(i, j int) {
			userInteractions[i], userInteractions[j] = userInteractions[j], userInteractions[i]
		})

		splitIdx := int(float64(len(userInteractions)) * trainRatio)
		if splitIdx < 1 && len(userInteractions) > 1 {
			splitIdx = 1
		}

		train = append(train, userInteractions[:splitIdx]...)
		test = append(test, userInteractions[splitIdx:]...)
	}
	return train, test
}

// FilterMinInteractions removes users with fewer than minInteractions interactions.
func FilterMinInteractions(data *LoadResult, minInteractions int) *LoadResult {
	// Count per-user interactions
	counts := make(map[string]int)
	for _, it := range data.Interactions {
		counts[it.UserID]++
	}

	// Filter
	var filtered []cf.Interaction
	userSet := make(map[string]struct{})
	itemSet := make(map[string]struct{})
	for _, it := range data.Interactions {
		if counts[it.UserID] >= minInteractions {
			filtered = append(filtered, it)
			userSet[it.UserID] = struct{}{}
			itemSet[it.ItemID] = struct{}{}
		}
	}

	userIDs := make([]string, 0, len(userSet))
	for uid := range userSet {
		userIDs = append(userIDs, uid)
	}
	itemIDs := make([]string, 0, len(itemSet))
	for iid := range itemSet {
		itemIDs = append(itemIDs, iid)
	}

	userItems := make(map[string]map[string]struct{})
	itemUsers := make(map[string]map[string]struct{})
	for _, it := range filtered {
		if userItems[it.UserID] == nil {
			userItems[it.UserID] = make(map[string]struct{})
		}
		userItems[it.UserID][it.ItemID] = struct{}{}

		if itemUsers[it.ItemID] == nil {
			itemUsers[it.ItemID] = make(map[string]struct{})
		}
		itemUsers[it.ItemID][it.UserID] = struct{}{}
	}

	return &LoadResult{
		Interactions: filtered,
		UserItems:    userItems,
		ItemUsers:    itemUsers,
		UserIDs:      userIDs,
		ItemIDs:      itemIDs,
	}
}

// BuildInteractions converts a slice of cf.Interaction to the format needed by BPR.
// It's a pass-through but ensures no duplicates.
func BuildInteractions(interactions []cf.Interaction) []cf.Interaction {
	seen := make(map[[2]string]struct{}, len(interactions))
	out := make([]cf.Interaction, 0, len(interactions))
	for _, it := range interactions {
		key := [2]string{it.UserID, it.ItemID}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, it)
	}
	return out
}

// ParseUint64 parses a string to uint64, used for timestamp fields.
func ParseUint64(s string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSpace(s), 10, 64)
}
