package eval

import (
	"context"
	"fmt"
	"math/rand"
	"sort"

	"go-rec/pkg/cf"
)

// EvalResult holds the result of one fold evaluation.
type EvalResult struct {
	Fold     int
	Baseline Metrics
	Enhanced Metrics
}

// EvaluatorConfig holds evaluation configuration.
type EvaluatorConfig struct {
	Folds      int // number of random splits (default 5)
	TopK       int // recall top-K (default 10)
	MinUser    int // minimum interactions per user
	MaxSamples int // max records to load (0 = all)
	CandidateN int // sampled candidate pool size (default 1000)
}

// DefaultEvaluatorConfig returns recommended defaults.
func DefaultEvaluatorConfig() EvaluatorConfig {
	return EvaluatorConfig{
		Folds:      5,
		TopK:       10,
		MinUser:    5,
		CandidateN: 1000,
	}
}

// Evaluator runs the offline evaluation pipeline.
type Evaluator struct {
	cfg  EvaluatorConfig
	data *LoadResult
}

// NewEvaluator creates a new evaluator with the given config.
func NewEvaluator(cfg EvaluatorConfig) *Evaluator {
	return &Evaluator{cfg: cfg}
}

// Run executes the full evaluation pipeline.
// Protocol: Sampled evaluation (BPR paper style).
// For each test user:
// 1. Take all test items as positives
// 2. Sample CandidateN random items as candidate pool
// 3. For each positive, rank it among the candidates
// 4. Compute HR@K and NDCG@K
func (e *Evaluator) Run(ctx context.Context, csvPath string) ([]EvalResult, Metrics, Metrics, *TTestResult, error) {
	fmt.Printf("[eval] loading data from %s\n", csvPath)
	data, err := LoadCSV(csvPath, e.cfg.MaxSamples)
	if err != nil {
		return nil, Metrics{}, Metrics{}, nil, fmt.Errorf("load data: %w", err)
	}
	fmt.Printf("[eval] loaded %d interactions, %d users, %d items\n",
		len(data.Interactions), len(data.UserIDs), len(data.ItemIDs))

	data = FilterMinInteractions(data, e.cfg.MinUser)
	fmt.Printf("[eval] after filtering: %d interactions, %d users, %d items\n",
		len(data.Interactions), len(data.UserIDs), len(data.ItemIDs))

	if len(data.UserIDs) == 0 {
		return nil, Metrics{}, Metrics{}, nil, fmt.Errorf("no users with enough interactions")
	}

	folds := e.cfg.Folds
	if folds <= 0 {
		folds = 5
	}

	results := make([]EvalResult, 0, folds)
	rng := rand.New(rand.NewSource(42))

	for fold := 0; fold < folds; fold++ {
		fmt.Printf("[eval] === Fold %d/%d ===\n", fold+1, folds)

		train, test := SplitTrainTest(data, fold, 0.8)
		fmt.Printf("[eval] train: %d, test: %d\n", len(train), len(test))

		// Build maps
		testUserItems := make(map[string][]string)
		for _, it := range test {
			testUserItems[it.UserID] = append(testUserItems[it.UserID], it.ItemID)
		}
		trainUserItems := make(map[string]map[string]struct{})
		for _, it := range train {
			if trainUserItems[it.UserID] == nil {
				trainUserItems[it.UserID] = make(map[string]struct{})
			}
			trainUserItems[it.UserID][it.ItemID] = struct{}{}
		}

		// Build item popularity ONLY from training data (avoid information leakage)
		itemPop := make(map[string]int)
		for _, it := range train {
			itemPop[it.ItemID]++
		}

		// --- Baseline ---
		baseMetrics := e.evaluateSampled(testUserItems, trainUserItems, data.ItemIDs, itemPop, rng, true)

		// --- Enhanced ---
		enhMetrics := e.evaluateEnhancedSampled(ctx, train, testUserItems, trainUserItems, data, rng)

		result := EvalResult{Fold: fold + 1, Baseline: baseMetrics, Enhanced: enhMetrics}
		results = append(results, result)
		fmt.Printf("[eval] fold %d: baseline NDCG@%d=%.4f HR@%d=%.4f | enhanced NDCG@%d=%.4f HR@%d=%.4f\n",
			fold+1, e.cfg.TopK, baseMetrics.NDCG10, e.cfg.TopK, baseMetrics.HR20,
			e.cfg.TopK, enhMetrics.NDCG10, e.cfg.TopK, enhMetrics.HR20)
	}

	avgBaseline := AverageMetrics(extractBaseline(results))
	avgEnhanced := AverageMetrics(extractEnhanced(results))

	ndcgB := extractField(extractBaseline(results), "ndcg")
	ndcgE := extractField(extractEnhanced(results), "ndcg")
	hrB := extractField(extractBaseline(results), "hr")
	hrE := extractField(extractEnhanced(results), "hr")

	ndcgT, ndcgSig := PairedTTest(ndcgE, ndcgB, 0.05)
	hrT, hrSig := PairedTTest(hrE, hrB, 0.05)

	tTest := &TTestResult{NDCGTStat: ndcgT, NDCGSignificant: ndcgSig, HRTStat: hrT, HRSignificant: hrSig}
	return results, avgBaseline, avgEnhanced, tTest, nil
}

// evaluateSampled for baseline (popularity).
func (e *Evaluator) evaluateSampled(testUserItems map[string][]string, trainUserItems map[string]map[string]struct{}, allItems []string, itemPop map[string]int, rng *rand.Rand, _ bool) Metrics {
	// Sort items by popularity
	type kv struct{ key string; value int }
	pairs := make([]kv, 0, len(itemPop))
	for k, v := range itemPop {
		pairs = append(pairs, kv{key: k, value: v})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].value > pairs[j].value })
	popRanking := make([]string, len(pairs))
	for i, p := range pairs {
		popRanking[i] = p.key
	}

	ndcgSum, hrSum := 0.0, 0.0
	count := 0

	for user, posItems := range testUserItems {
		trainSet := trainUserItems[user]
		// For each positive item, create a test set: 1 positive + CandidateN negatives
		sortedItems := preSortedItems(allItems, itemPop)
		for _, posItem := range posItems {
			negCandidates := e.sampleCandidatePool(sortedItems, trainSet, posItem)
			// Build candidate list: positive + negatives
			candList := append([]string{posItem}, negCandidates...)

			// Rank by popularity
			type kv2 struct{ item string; score int }
			scored := make([]kv2, len(candList))
			for i, item := range candList {
				scored[i] = kv2{item: item, score: itemPop[item]}
			}
			sort.Slice(scored, func(i, j int) bool { return scored[i].score > scored[j].score })
			ranked := make([]string, len(scored))
			for i, s := range scored {
				ranked[i] = s.item
			}

			relSet := map[string]struct{}{posItem: {}}
			ndcgSum += NDCG(relSet, ranked, e.cfg.TopK)
			hrSum += HR(relSet, ranked, e.cfg.TopK)
			count++
		}
	}

	if count == 0 {
		return Metrics{}
	}
	n := float64(count)
	return Metrics{NDCG10: ndcgSum / n, HR20: hrSum / n}
}

// evaluateSampled for enhanced (three-way recall).
func (e *Evaluator) evaluateEnhancedSampled(ctx context.Context, train []cf.Interaction, testUserItems map[string][]string, trainUserItems map[string]map[string]struct{}, data *LoadResult, rng *rand.Rand) Metrics {
	recaller := cf.NewCFRecaller()
	cfg := cf.DefaultCFTrainConfig()
	cfg.BPRParams.NEpochs = 200
	cfg.BPRParams.NFactors = 16
	recaller.Train(train, cfg)

	// Build co-occurrence
	itemCooccur := make(map[string]map[string]int)
	userItems := make(map[string][]string)
	for _, it := range train {
		userItems[it.UserID] = append(userItems[it.UserID], it.ItemID)
	}
	for _, items := range userItems {
		for i, a := range items {
			for _, b := range items[i+1:] {
				if itemCooccur[a] == nil { itemCooccur[a] = make(map[string]int) }
				if itemCooccur[b] == nil { itemCooccur[b] = make(map[string]int) }
				itemCooccur[a][b]++
				itemCooccur[b][a]++
			}
		}
	}

	itemPop := make(map[string]int)
	for _, it := range train { itemPop[it.ItemID]++ }

	ndcgSum, hrSum := 0.0, 0.0
	count := 0

	for user, posItems := range testUserItems {
		trainSet := trainUserItems[user]
		// For each positive item, create a test set: 1 positive + CandidateN negatives
		sortedItems := preSortedItems(data.ItemIDs, itemPop)
		for _, posItem := range posItems {
			negCandidates := e.sampleCandidatePool(sortedItems, trainSet, posItem)
			candList := append([]string{posItem}, negCandidates...)

			// Score each candidate
			scores := make(map[string]float64, len(candList))
			for _, item := range candList {
				var score float64
				// CF
				score += 0.4 * float64(recaller.Predict(user, item))
				// Content
				for trainItem := range trainSet {
					if co, ok := itemCooccur[trainItem]; ok {
						if c, ok := co[item]; ok {
							s := float64(c) / float64(len(co))
							if s > score*0.3 { score += 0.3 * s }
						}
					}
				}
				// Profile: item popularity among similar users
				score += 0.3 * float64(itemPop[item]) / float64(len(data.Interactions))
				scores[item] = score
			}

			type kv struct{ item string; score float64 }
			pairs := make([]kv, 0, len(scores))
			for k, v := range scores { pairs = append(pairs, kv{item: k, score: v}) }
			sort.Slice(pairs, func(i, j int) bool { return pairs[i].score > pairs[j].score })
			ranked := make([]string, len(pairs))
			for i, p := range pairs { ranked[i] = p.item }

			relSet := map[string]struct{}{posItem: {}}
			ndcgSum += NDCG(relSet, ranked, e.cfg.TopK)
			hrSum += HR(relSet, ranked, e.cfg.TopK)
			count++
		}
	}

	if count == 0 { return Metrics{} }
	n := float64(count)
	return Metrics{NDCG10: ndcgSum / n, HR20: hrSum / n}
}

// preSortedItems returns items sorted by popularity descending (cached per fold).
func preSortedItems(allItems []string, itemPop map[string]int) []string {
	type kv struct{ item string; pop int }
	pairs := make([]kv, len(allItems))
	for i, item := range allItems {
		pairs[i] = kv{item: item, pop: itemPop[item]}
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].pop > pairs[j].pop })
	sorted := make([]string, len(pairs))
	for i, p := range pairs { sorted[i] = p.item }
	return sorted
}

func (e *Evaluator) sampleCandidatePool(sortedItems []string, trainSet map[string]struct{}, posItem string) []string {
	posSet := map[string]struct{}{posItem: {}}
	result := make([]string, 0, e.cfg.CandidateN)
	for _, item := range sortedItems {
		if len(result) >= e.cfg.CandidateN { break }
		if _, inTrain := trainSet[item]; inTrain { continue }
		if _, isPos := posSet[item]; isPos { continue }
		result = append(result, item)
	}
	return result
}

func extractBaseline(results []EvalResult) []Metrics {
	out := make([]Metrics, len(results))
	for i, r := range results { out[i] = r.Baseline }
	return out
}

func extractEnhanced(results []EvalResult) []Metrics {
	out := make([]Metrics, len(results))
	for i, r := range results { out[i] = r.Enhanced }
	return out
}

func extractField(metrics []Metrics, field string) []float64 {
	out := make([]float64, len(metrics))
	for i, m := range metrics {
		switch field {
		case "ndcg": out[i] = m.NDCG10
		case "hr": out[i] = m.HR20
		}
	}
	return out
}

type TTestResult struct {
	NDCGTStat, HRTStat float64
	NDCGSignificant, HRSignificant bool
}

func min(a, b int) int { if a < b { return a }; return b }
func max(a, b int) int { if a > b { return a }; return b }
