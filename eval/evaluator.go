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
	Folds      int
	TopK       int
	MinUser    int
	MaxSamples int
	CandidateN int
}

// DefaultEvaluatorConfig returns recommended defaults.
func DefaultEvaluatorConfig() EvaluatorConfig {
	return EvaluatorConfig{
		Folds:      5,
		TopK:       10,
		MinUser:    20,
		CandidateN: 99,
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

// Run executes evaluation comparing BPR personalized ranking vs random baseline.
// Protocol: leave-one-out + 99 random negatives.
// Baseline: random ranking (expected HR@10 = 10/100 = 0.10)
// Enhanced: BPR personalized ranking (should be significantly better)
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

	// Group by user
	userInteractions := make(map[string][]string)
	for _, it := range data.Interactions {
		userInteractions[it.UserID] = append(userInteractions[it.UserID], it.ItemID)
	}

	// Leave-one-out
	var testCases []struct {
		user     string
		posItem  string
		trainSet map[string]struct{}
	}
	for user, items := range userInteractions {
		if len(items) < 2 {
			continue
		}
		posItem := items[len(items)-1]
		trainSet := make(map[string]struct{}, len(items)-1)
		for _, item := range items[:len(items)-1] {
			trainSet[item] = struct{}{}
		}
		testCases = append(testCases, struct {
			user     string
			posItem  string
			trainSet map[string]struct{}
		}{user: user, posItem: posItem, trainSet: trainSet})
	}

	// Build train interactions
	var fullTrain []cf.Interaction
	for user, items := range userInteractions {
		for _, item := range items[:len(items)-1] {
			fullTrain = append(fullTrain, cf.Interaction{UserID: user, ItemID: item})
		}
	}

	fmt.Printf("[eval] leave-one-out: %d test cases, training BPR...\n", len(testCases))

	// Train BPR
	recaller := cf.NewCFRecaller()
	cfg := cf.DefaultCFTrainConfig()
	cfg.BPRParams.NEpochs = 200
	cfg.BPRParams.NFactors = 16
	recaller.Train(fullTrain, cfg)
	fmt.Printf("[eval] BPR trained: %d users, %d items\n", len(fullTrain), len(data.ItemIDs))

	folds := e.cfg.Folds
	if folds <= 0 {
		folds = 5
	}

	// Build item popularity from training data
	itemPop := make(map[string]int)
	for _, it := range fullTrain {
		itemPop[it.ItemID]++
	}

	// Build item co-occurrence for content recall
	itemCooccur := make(map[string]map[string]int)
	trainUserItems := make(map[string][]string)
	for _, it := range fullTrain {
		trainUserItems[it.UserID] = append(trainUserItems[it.UserID], it.ItemID)
	}
	for _, items := range trainUserItems {
		for i, a := range items {
			for _, b := range items[i+1:] {
				if itemCooccur[a] == nil {
					itemCooccur[a] = make(map[string]int)
				}
				if itemCooccur[b] == nil {
					itemCooccur[b] = make(map[string]int)
				}
				itemCooccur[a][b]++
				itemCooccur[b][a]++
			}
		}
	}

	rng := rand.New(rand.NewSource(42))
	results := make([]EvalResult, 0, folds)

	for fold := 0; fold < folds; fold++ {
		fmt.Printf("[eval] === Fold %d/%d ===\n", fold+1, folds)

		// --- Baseline: pure CF (BPR personalized ranking, no Content/Profile signals) ---
		baseMetrics := e.evaluatePureCF(testCases, data.ItemIDs, recaller, rng)

		// --- Enhanced: three-way recall (CF + Content + Profile) ---
		enhMetrics := e.evaluateThreeWay(testCases, data.ItemIDs, recaller, itemCooccur, trainUserItems, itemPop, len(fullTrain), rng)

		result := EvalResult{Fold: fold + 1, Baseline: baseMetrics, Enhanced: enhMetrics}
		results = append(results, result)
		fmt.Printf("[eval] fold %d: random NDCG@%d=%.4f HR@%d=%.4f | BPR NDCG@%d=%.4f HR@%d=%.4f\n",
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

// evaluatePureCF evaluates pure collaborative filtering baseline using only BPR scores.
// No Content or Profile signals are used — this isolates the CF signal.
func (e *Evaluator) evaluatePureCF(testCases []struct {
	user     string
	posItem  string
	trainSet map[string]struct{}
}, allItems []string, recaller *cf.CFRecaller, rng *rand.Rand) Metrics {

	negN := e.cfg.CandidateN
	ndcgSum, hrSum := 0.0, 0.0
	count := 0

	for _, tc := range testCases {
		negItems := sampleNeg(allItems, tc.trainSet, tc.posItem, negN, rng)
		candidates := append([]string{tc.posItem}, negItems...)

		// Score each candidate using only BPR personalized score (pure CF)
		type scored struct {
			item  string
			score float64
		}
		pairs := make([]scored, len(candidates))
		for i, item := range candidates {
			pairs[i] = scored{item: item, score: float64(recaller.Predict(tc.user, item))}
		}

		// Sort by BPR score descending
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].score > pairs[j].score })
		ranked := make([]string, len(pairs))
		for i, p := range pairs {
			ranked[i] = p.item
		}

		relSet := map[string]struct{}{tc.posItem: {}}
		ndcgSum += NDCG(relSet, ranked, e.cfg.TopK)
		hrSum += HR(relSet, ranked, e.cfg.TopK)
		count++
	}

	if count == 0 {
		return Metrics{}
	}
	n := float64(count)
	return Metrics{NDCG10: ndcgSum / n, HR20: hrSum / n}
}

// evaluateThreeWay evaluates three-way recall: CF + Content + Profile.
// CF: BPR predicted score (personalized collaborative filtering)
// Content: item co-occurrence similarity (items frequently bought together)
// Profile: user similarity (items popular among similar users)
func (e *Evaluator) evaluateThreeWay(testCases []struct {
	user     string
	posItem  string
	trainSet map[string]struct{}
}, allItems []string, recaller *cf.CFRecaller, itemCooccur map[string]map[string]int, userItems map[string][]string, itemPop map[string]int, totalInteractions int, rng *rand.Rand) Metrics {

	negN := e.cfg.CandidateN
	ndcgSum, hrSum := 0.0, 0.0
	count := 0

	for _, tc := range testCases {
		negItems := sampleNeg(allItems, tc.trainSet, tc.posItem, negN, rng)
		candidates := append([]string{tc.posItem}, negItems...)

		// Score each candidate with three-way recall
		type scored struct {
			item  string
			score float64
		}
		pairs := make([]scored, len(candidates))
		for i, item := range candidates {
			var score float64

			// Path 1: CF (BPR personalized score)
			cfScore := float64(recaller.Predict(tc.user, item))
			score += 0.4 * cfScore

			// Path 2: Content (co-occurrence with user's training items)
			contentScore := 0.0
			for trainItem := range tc.trainSet {
				if coItems, ok := itemCooccur[trainItem]; ok {
					if c, ok := coItems[item]; ok {
						s := float64(c) / float64(len(coItems))
						if s > contentScore {
							contentScore = s
						}
					}
				}
			}
			score += 0.3 * contentScore

			// Path 3: Profile (items popular among similar users)
			profileScore := 0.0
			similarCount := 0
			for trainItem := range tc.trainSet {
				for _, simUser := range userItems[trainItem] {
					if simUser != tc.user {
						similarCount++
					}
				}
			}
			if similarCount > 0 {
				profileScore = float64(len(tc.trainSet)) / float64(similarCount+1)
			}
			score += 0.3 * profileScore * float64(itemPop[item]) / float64(totalInteractions+1)

			pairs[i] = scored{item: item, score: score}
		}

		// Sort by combined score descending
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].score > pairs[j].score })
		ranked := make([]string, len(pairs))
		for i, p := range pairs {
			ranked[i] = p.item
		}

		relSet := map[string]struct{}{tc.posItem: {}}
		ndcgSum += NDCG(relSet, ranked, e.cfg.TopK)
		hrSum += HR(relSet, ranked, e.cfg.TopK)
		count++
	}

	if count == 0 {
		return Metrics{}
	}
	n := float64(count)
	return Metrics{NDCG10: ndcgSum / n, HR20: hrSum / n}
}

func sampleNeg(allItems []string, trainSet map[string]struct{}, posItem string, n int, rng *rand.Rand) []string {
	// Build pool of eligible items (not in trainSet, not posItem)
	pool := make([]string, 0, len(allItems))
	for _, item := range allItems {
		if item == posItem {
			continue
		}
		if _, inTrain := trainSet[item]; inTrain {
			continue
		}
		pool = append(pool, item)
	}
	if len(pool) == 0 {
		return nil
	}
	// Shuffle and take first n
	rng.Shuffle(len(pool), func(i, j int) {
		pool[i], pool[j] = pool[j], pool[i]
	})
	if n > len(pool) {
		n = len(pool)
	}
	return pool[:n]
}

func extractBaseline(results []EvalResult) []Metrics {
	out := make([]Metrics, len(results))
	for i, r := range results {
		out[i] = r.Baseline
	}
	return out
}

func extractEnhanced(results []EvalResult) []Metrics {
	out := make([]Metrics, len(results))
	for i, r := range results {
		out[i] = r.Enhanced
	}
	return out
}

func extractField(metrics []Metrics, field string) []float64 {
	out := make([]float64, len(metrics))
	for i, m := range metrics {
		switch field {
		case "ndcg":
			out[i] = m.NDCG10
		case "hr":
			out[i] = m.HR20
		}
	}
	return out
}

type TTestResult struct {
	NDCGTStat, HRTStat             float64
	NDCGSignificant, HRSignificant bool
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
