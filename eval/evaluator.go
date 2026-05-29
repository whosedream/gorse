package eval

import (
	"context"
	"fmt"
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
	TopK       int // recall top-K (default 20)
	MinUser    int // minimum interactions per user
	MaxSamples int // max records to load (0 = all)
}

// DefaultEvaluatorConfig returns recommended defaults.
func DefaultEvaluatorConfig() EvaluatorConfig {
	return EvaluatorConfig{
		Folds:   5,
		TopK:    20,
		MinUser: 5,
	}
}

// Evaluator runs the offline evaluation pipeline.
type Evaluator struct {
	cfg   EvaluatorConfig
	data  *LoadResult
}

// NewEvaluator creates a new evaluator with the given config.
func NewEvaluator(cfg EvaluatorConfig) *Evaluator {
	return &Evaluator{cfg: cfg}
}

// Run executes the full evaluation pipeline:
// 1. Load data from CSV
// 2. Filter users with too few interactions
// 3. For each fold: train BPR, build HNSW, evaluate on test set
// 4. Compute paired t-test for statistical significance
func (e *Evaluator) Run(ctx context.Context, csvPath string) ([]EvalResult, Metrics, Metrics, *TTestResult, error) {
	fmt.Printf("[eval] loading data from %s\n", csvPath)
	data, err := LoadCSV(csvPath, e.cfg.MaxSamples)
	if err != nil {
		return nil, Metrics{}, Metrics{}, nil, fmt.Errorf("load data: %w", err)
	}
	fmt.Printf("[eval] loaded %d interactions, %d users, %d items\n",
		len(data.Interactions), len(data.UserIDs), len(data.ItemIDs))

	// Filter users with too few interactions
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

	for fold := 0; fold < folds; fold++ {
		fmt.Printf("[eval] === Fold %d/%d ===\n", fold+1, folds)

		train, test := SplitTrainTest(data, fold, 0.8)
		fmt.Printf("[eval] train: %d interactions, test: %d interactions\n", len(train), len(test))

		// Build test user -> relevant items map
		testUserItems := make(map[string]map[string]struct{})
		for _, it := range test {
			if testUserItems[it.UserID] == nil {
				testUserItems[it.UserID] = make(map[string]struct{})
			}
			testUserItems[it.UserID][it.ItemID] = struct{}{}
		}

		// --- Baseline: random ranking (popularity-based) ---
		baselineMetrics := e.evaluateBaseline(data, testUserItems)

		// --- Enhanced: BPR + HNSW ---
		enhancedMetrics := e.evaluateEnhanced(ctx, train, testUserItems, data)

		result := EvalResult{
			Fold:     fold + 1,
			Baseline: baselineMetrics,
			Enhanced: enhancedMetrics,
		}
		results = append(results, result)
		fmt.Printf("[eval] fold %d: baseline NDCG@10=%.4f HR@20=%.4f | enhanced NDCG@10=%.4f HR@20=%.4f\n",
			fold+1, baselineMetrics.NDCG10, baselineMetrics.HR20,
			enhancedMetrics.NDCG10, enhancedMetrics.HR20)
	}

	// Aggregate
	avgBaseline := AverageMetrics(extractBaseline(results))
	avgEnhanced := AverageMetrics(extractEnhanced(results))

	// Paired t-test
	ndcgBaseline := extractField(extractBaseline(results), "ndcg")
	ndcgEnhanced := extractField(extractEnhanced(results), "ndcg")
	hrBaseline := extractField(extractBaseline(results), "hr")
	hrEnhanced := extractField(extractEnhanced(results), "hr")

	ndcgTStat, ndcgSig := PairedTTest(ndcgEnhanced, ndcgBaseline, 0.05)
	hrTStat, hrSig := PairedTTest(hrEnhanced, hrBaseline, 0.05)

	tTestResult := &TTestResult{
		NDCGTStat:      ndcgTStat,
		NDCGSignificant: ndcgSig,
		HRTStat:        hrTStat,
		HRSignificant:  hrSig,
	}

	return results, avgBaseline, avgEnhanced, tTestResult, nil
}

// evaluateBaseline evaluates a popularity-based baseline.
// Items are ranked by their frequency in the training data.
func (e *Evaluator) evaluateBaseline(data *LoadResult, testUserItems map[string]map[string]struct{}) Metrics {
	// Count item popularity
	itemCount := make(map[string]int)
	for _, it := range data.Interactions {
		itemCount[it.ItemID]++
	}

	// Rank items by popularity (descending)
	type kv struct {
		key   string
		value int
	}
	pairs := make([]kv, 0, len(itemCount))
	for k, v := range itemCount {
		pairs = append(pairs, kv{key: k, value: v})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].value > pairs[j].value
	})
	ranked := make([]string, len(pairs))
	for i, p := range pairs {
		ranked[i] = p.key
	}

	// Evaluate each test user
	var ndcgSum, hrSum float64
	count := 0
	for user, relItems := range testUserItems {
		_ = user
		if len(relItems) == 0 {
			continue
		}
		ndcgSum += NDCG(relItems, ranked, 10)
		hrSum += HR(relItems, ranked, 20)
		count++
	}

	if count == 0 {
		return Metrics{}
	}
	n := float64(count)
	return Metrics{
		NDCG10: ndcgSum / n,
		HR20:   hrSum / n,
	}
}

// evaluateEnhanced evaluates the BPR + HNSW enhanced model.
func (e *Evaluator) evaluateEnhanced(ctx context.Context, train []cf.Interaction, testUserItems map[string]map[string]struct{}, data *LoadResult) Metrics {
	// Train BPR + build HNSW
	recaller := cf.NewCFRecaller()
	cfg := cf.DefaultCFTrainConfig()
	cfg.BPRParams.NEpochs = 50
	cfg.BPRParams.NFactors = 16

	trainInteractions := BuildInteractions(train)
	recaller.Train(trainInteractions, cfg)

	// Evaluate each test user
	ndcgSum := 0.0
	hrSum := 0.0
	count := 0

	for user, relItems := range testUserItems {
		if len(relItems) == 0 {
			continue
		}

		// Get recommendations
		candidates, err := recaller.Recall(ctx, user, e.cfg.TopK)
		if err != nil || len(candidates) == 0 {
			continue
		}

		// Convert to ranked list
		ranked := make([]string, len(candidates))
		for i, c := range candidates {
			ranked[i] = c.ItemID
		}

		ndcgSum += NDCG(relItems, ranked, 10)
		hrSum += HR(relItems, ranked, 20)
		count++
	}

	if count == 0 {
		return Metrics{}
	}
	n := float64(count)
	return Metrics{
		NDCG10: ndcgSum / n,
		HR20:   hrSum / n,
	}
}

// TTestResult holds the results of paired t-tests.
type TTestResult struct {
	NDCGTStat       float64
	NDCGSignificant bool
	HRTStat         float64
	HRSignificant   bool
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
