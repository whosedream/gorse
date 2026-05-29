package eval

import (
	"math"
	"sort"
)

// Metrics holds evaluation metric values.
type Metrics struct {
	NDCG10 float64
	HR20   float64
}

// NDCG computes Normalized Discounted Cumulative Gain at K.
// relevance is the ground-truth set of relevant items for a user.
// ranked is the ordered list of recommended item IDs (ranked by score descending).
func NDCG(relevance map[string]struct{}, ranked []string, k int) float64 {
	if k <= 0 || len(ranked) == 0 || len(relevance) == 0 {
		return 0
	}

	// DCG: sum of 1/log2(rank+2) for relevant items in top-k
	dcg := 0.0
	for i := 0; i < k && i < len(ranked); i++ {
		if _, ok := relevance[ranked[i]]; ok {
			dcg += 1.0 / math.Log2(float64(i+2)) // log2(rank+1+1) = log2(rank+2)
		}
	}

	// IDCG: ideal DCG (all relevant items at the top)
	idealCount := len(relevance)
	if idealCount > k {
		idealCount = k
	}
	idcg := 0.0
	for i := 0; i < idealCount; i++ {
		idcg += 1.0 / math.Log2(float64(i+2))
	}

	if idcg == 0 {
		return 0
	}
	return dcg / idcg
}

// HR computes Hit Rate at K.
// relevance is the ground-truth set of relevant items.
// ranked is the ordered list of recommended item IDs.
func HR(relevance map[string]struct{}, ranked []string, k int) float64 {
	if k <= 0 || len(ranked) == 0 || len(relevance) == 0 {
		return 0
	}

	for i := 0; i < k && i < len(ranked); i++ {
		if _, ok := relevance[ranked[i]]; ok {
			return 1.0
		}
	}
	return 0.0
}

// RankItemsByScore sorts item IDs by their scores in descending order and returns the ranked list.
func RankItemsByScore(scores map[string]float64) []string {
	type kv struct {
		key   string
		value float64
	}
	pairs := make([]kv, 0, len(scores))
	for k, v := range scores {
		pairs = append(pairs, kv{key: k, value: v})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].value > pairs[j].value
	})
	ranked := make([]string, len(pairs))
	for i, p := range pairs {
		ranked[i] = p.key
	}
	return ranked
}

// AverageMetrics computes element-wise average of a slice of Metrics.
func AverageMetrics(results []Metrics) Metrics {
	if len(results) == 0 {
		return Metrics{}
	}
	var sumNDCG, sumHR float64
	for _, m := range results {
		sumNDCG += m.NDCG10
		sumHR += m.HR20
	}
	n := float64(len(results))
	return Metrics{
		NDCG10: sumNDCG / n,
		HR20:   sumHR / n,
	}
}

// PairedTTest performs a paired two-sample t-test.
// Returns the t-statistic and whether p < alpha (one-tailed).
// Tests whether mean(a) > mean(b).
func PairedTTest(a, b []float64, alpha float64) (tStat float64, significant bool) {
	n := len(a)
	if n != len(b) || n < 2 {
		return 0, false
	}

	// Compute differences
	diffs := make([]float64, n)
	var sumD float64
	for i := range a {
		diffs[i] = a[i] - b[i]
		sumD += diffs[i]
	}

	meanD := sumD / float64(n)

	// Compute std dev of differences
	var ss float64
	for _, d := range diffs {
		ss += (d - meanD) * (d - meanD)
	}
	stdD := math.Sqrt(ss / float64(n-1))

	if stdD == 0 {
		return 0, false
	}

	tStat = meanD / (stdD / math.Sqrt(float64(n)))

	// Approximate p-value using t-distribution with n-1 degrees of freedom
	// Using the relationship: P(T > t) = 0.5 * I_x(df/2, 0.5) where x = df/(df+t^2)
	// For degrees of freedom >= 30, use normal approximation
	pValue := tPValue(tStat, n-1)

	return tStat, pValue < alpha
}

// tPValue returns the one-tailed p-value P(T > t) for Student's t-distribution.
func tPValue(t float64, df int) float64 {
	x := float64(df) / (float64(df) + t*t)
	cdf := 1.0 - 0.5*betaIncomplete(float64(df)/2.0, 0.5, x)
	// P(T > t) = 1 - CDF(t)
	if t >= 0 {
		return 1.0 - cdf
	}
	return cdf
}

// betaIncomplete computes the regularized incomplete beta function I_x(a, b).
func betaIncomplete(a, b, x float64) float64 {
	if x < 0 || x > 1 {
		return 0
	}
	if x == 0 || x == 1 {
		return x
	}

	// Use continued fraction (Lentz's method)
	lbeta, _ := math.Lgamma(a)
	lb, _ := math.Lgamma(b)
	lab, _ := math.Lgamma(a + b)
	lbz := math.Exp(lbeta + lb - lab)

	front := math.Pow(x, a) * math.Pow(1-x, b) / (a * lbz)

	// Continued fraction
	qab := a + b
	qap := a + 1.0
	qam := a - 1.0

	c := 1.0
	d := 1.0 - qab*x/qap
	if math.Abs(d) < 1e-30 {
		d = 1e-30
	}
	d = 1.0 / d
	h := d

	for m := 1; m <= 200; m++ {
		m2 := 2 * m

		// Even step
		aa := float64(m) * (b - float64(m)) * x / ((qam + float64(m2)) * (a + float64(m2)))
		d = 1.0 + aa*d
		if math.Abs(d) < 1e-30 {
			d = 1e-30
		}
		c = 1.0 + aa/c
		if math.Abs(c) < 1e-30 {
			c = 1e-30
		}
		d = 1.0 / d
		h *= d * c

		// Odd step
		aa = -(a + float64(m)) * (qab + float64(m)) * x / ((a + float64(m2)) * (qap + float64(m2)))
		d = 1.0 + aa*d
		if math.Abs(d) < 1e-30 {
			d = 1e-30
		}
		c = 1.0 + aa/c
		if math.Abs(c) < 1e-30 {
			c = 1e-30
		}
		d = 1.0 / d
		del := d * c
		h *= del

		if math.Abs(del-1.0) < 1e-10 {
			break
		}
	}

	return front * h
}
