package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"go-rec/eval"
)

func main() {
	csvPath := flag.String("csv", "", "path to Amazon Reviews CSV")
	folds := flag.Int("folds", 5, "number of random folds")
	topK := flag.Int("topk", 20, "recall top-K")
	minUser := flag.Int("min-user", 5, "minimum interactions per user")
	maxSamples := flag.Int("max-samples", 0, "max records to load (0 = all)")
	flag.Parse()

	if *csvPath == "" {
		fmt.Fprintln(os.Stderr, "usage: eval-cli -csv <path>")
		flag.PrintDefaults()
		os.Exit(1)
	}

	cfg := eval.EvaluatorConfig{
		Folds:      *folds,
		TopK:       *topK,
		MinUser:    *minUser,
		MaxSamples: *maxSamples,
	}

	evaluator := eval.NewEvaluator(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	fmt.Println("=== Offline Evaluation Pipeline ===")
	fmt.Printf("CSV:        %s\n", *csvPath)
	fmt.Printf("Folds:      %d\n", *folds)
	fmt.Printf("TopK:       %d\n", *topK)
	fmt.Printf("MinUser:    %d\n", *minUser)
	fmt.Printf("MaxSamples: %d (0=all)\n", *maxSamples)
	fmt.Println()

	results, avgBaseline, avgEnhanced, tTest, err := evaluator.Run(ctx, *csvPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "evaluation failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n=== Per-Fold Results ===")
	fmt.Printf("%-6s  %-20s  %-20s\n", "Fold", "Baseline", "Enhanced")
	fmt.Printf("%-6s  %-20s  %-20s\n", "----", "--------", "--------")
	for _, r := range results {
		fmt.Printf("%-6d  NDCG@10=%.4f HR@20=%.4f  NDCG@10=%.4f HR@20=%.4f\n",
			r.Fold,
			r.Baseline.NDCG10, r.Baseline.HR20,
			r.Enhanced.NDCG10, r.Enhanced.HR20)
	}

	fmt.Println("\n=== Average Results ===")
	fmt.Printf("Baseline: NDCG@10=%.4f  HR@20=%.4f\n", avgBaseline.NDCG10, avgBaseline.HR20)
	fmt.Printf("Enhanced: NDCG@10=%.4f  HR@20=%.4f\n", avgEnhanced.NDCG10, avgEnhanced.HR20)

	fmt.Println("\n=== Statistical Significance (paired t-test, alpha=0.05) ===")
	fmt.Printf("NDCG@10: t=%.4f, significant=%v\n", tTest.NDCGTStat, tTest.NDCGSignificant)
	fmt.Printf("HR@20:   t=%.4f, significant=%v\n", tTest.HRTStat, tTest.HRSignificant)

	// Print improvement
	if avgBaseline.NDCG10 > 0 {
		ndcgImp := (avgEnhanced.NDCG10 - avgBaseline.NDCG10) / avgBaseline.NDCG10 * 100
		fmt.Printf("\nNDCG@10 improvement: %.2f%%\n", ndcgImp)
	}
	if avgBaseline.HR20 > 0 {
		hrImp := (avgEnhanced.HR20 - avgBaseline.HR20) / avgBaseline.HR20 * 100
		fmt.Printf("HR@20 improvement:   %.2f%%\n", hrImp)
	}
}
