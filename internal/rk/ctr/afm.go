package ctr

import (
	"context"
	"math"
	"math/rand"
	"sync"
)

// AFMConfig holds hyperparameters for the Adaptive Factorization Machine.
type AFMConfig struct {
	// NumFeatures is the dimension of the input feature vectors.
	NumFeatures int
	// NumFactors is the latent factor dimension for feature interactions.
	NumFactors int
	// LearningRate is the initial Adam learning rate.
	LearningRate float32
	// L2Reg is the L2 regularization strength.
	L2Reg float32
	// AdamEps is the epsilon for Adam numerical stability.
	AdamEps float32
}

// DefaultAFMConfig returns production-ready defaults.
func DefaultAFMConfig() AFMConfig {
	return AFMConfig{
		NumFeatures:  64,
		NumFactors:   16,
		LearningRate: 0.001,
		L2Reg:        1e-5,
		AdamEps:      1e-8,
	}
}

// AFM implements an Adaptive Factorization Machine for CTR prediction.
// Formula: y = sigmoid(w0 + sum(wi*xi) + sum_i sum_{j>i} <vi,vj> * xi * xj)
// Trained with Adam optimizer.
type AFM struct {
	cfg AFMConfig

	mu sync.RWMutex

	// Model parameters
	bias     float32          // w0: global bias
	linear   []float32        // wi: first-order weights
	factors  [][]float32      // vi: second-order latent factors (NumFeatures x NumFactors)

	// Adam state for linear weights
	mLin []float32 // first moment
	vLin []float32 // second moment

	// Adam state for factor matrix
	mFac [][]float32 // first moment per factor row
	vFac [][]float32 // second moment per factor row

	t int64 // Adam timestep
}

// NewAFM creates and initializes an AFM model with random weights.
func NewAFM(cfg AFMConfig) *AFM {
	if cfg.NumFeatures <= 0 {
		cfg.NumFeatures = 64
	}
	if cfg.NumFactors <= 0 {
		cfg.NumFactors = 16
	}
	if cfg.LearningRate <= 0 {
		cfg.LearningRate = 0.001
	}
	if cfg.AdamEps <= 0 {
		cfg.AdamEps = 1e-8
	}

	rng := rand.New(rand.NewSource(42))
	scale := float32(1.0 / math.Sqrt(float64(cfg.NumFactors)))

	linear := make([]float32, cfg.NumFeatures)
	factors := make([][]float32, cfg.NumFeatures)
	mLin := make([]float32, cfg.NumFeatures)
	vLin := make([]float32, cfg.NumFeatures)
	mFac := make([][]float32, cfg.NumFeatures)
	vFac := make([][]float32, cfg.NumFeatures)

	for i := 0; i < cfg.NumFeatures; i++ {
		factors[i] = make([]float32, cfg.NumFactors)
		mFac[i] = make([]float32, cfg.NumFactors)
		vFac[i] = make([]float32, cfg.NumFactors)
		for k := 0; k < cfg.NumFactors; k++ {
			factors[i][k] = rng.Float32()*2*scale - scale
		}
	}

	return &AFM{
		cfg:    cfg,
		bias:   0,
		linear: linear,
		factors: factors,
		mLin:   mLin,
		vLin:   vLin,
		mFac:   mFac,
		vFac:   vFac,
	}
}

// sigmoid computes the logistic sigmoid function.
func sigmoid(x float32) float32 {
	if x >= 0 {
		return 1.0 / (1.0 + float32(math.Exp(float64(-x))))
	}
	ex := float32(math.Exp(float64(x)))
	return ex / (1.0 + ex)
}

// predict computes the raw FM score before sigmoid: w0 + <w,x> + sum interactions.
func (m *AFM) predict(features []float32) float32 {
	score := m.bias
	n := len(features)
	if n > m.cfg.NumFeatures {
		n = m.cfg.NumFeatures
	}

	// First-order: sum(wi * xi)
	for i := 0; i < n; i++ {
		if features[i] != 0 {
			score += m.linear[i] * features[i]
		}
	}

	// Second-order interactions via latent factors
	// For each factor k: 0.5 * (sum_i(v_ik * x_i))^2 - 0.5 * sum_i(v_ik^2 * x_i^2)
	for k := 0; k < m.cfg.NumFactors; k++ {
		sumVK := float32(0)
		sumVK2 := float32(0)
		for i := 0; i < n; i++ {
			if features[i] != 0 {
				vik := m.factors[i][k]
				sumVK += vik * features[i]
				sumVK2 += vik * vik * features[i] * features[i]
			}
		}
		score += 0.5 * (sumVK*sumVK - sumVK2)
	}

	return score
}

// Predict returns the CTR probability in [0,1] for a single sample.
func (m *AFM) Predict(features []float32) float32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return sigmoid(m.predict(features))
}

// TrainBatch performs one step of Adam optimization on a mini-batch.
// labels should be 0 or 1, one per sample. Returns the batch log-loss.
func (m *AFM) TrainBatch(batch [][]float32, labels []float32) float32 {
	if len(batch) != len(labels) || len(batch) == 0 {
		return 0
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.t++
	beta1 := float32(0.9)
	beta2 := float32(0.999)
	lr := m.cfg.LearningRate
	eps := m.cfg.AdamEps
	l2 := m.cfg.L2Reg

	bc1 := float32(1.0) - float32(math.Pow(float64(beta1), float64(m.t)))
	bc2 := float32(1.0) - float32(math.Pow(float64(beta2), float64(m.t)))

	// Accumulate gradients
	n := m.cfg.NumFeatures
	dBias := float32(0)
	dLin := make([]float32, n)
	// For factor gradients, use a per-sample approach to avoid O(batch * F * K) memory
	dFac := make([][]float32, n)
	for i := 0; i < n; i++ {
		dFac[i] = make([]float32, m.cfg.NumFactors)
	}

	totalLoss := float32(0)

	for s := 0; s < len(batch); s++ {
		features := batch[s]
		label := labels[s]
		featLen := len(features)
		if featLen > n {
			featLen = n
		}

		p := sigmoid(m.predict(features))
		// Gradient of log-loss: (p - y)
		err := p - label
		totalLoss -= label*float32(math.Log(float64(p)+1e-7)) + (1-label)*float32(math.Log(float64(1-p)+1e-7))

		dBias += err

		// First-order gradient + L2
		for i := 0; i < featLen; i++ {
			if features[i] != 0 {
				dLin[i] += err * features[i] + l2*m.linear[i]
			}
		}

		// Second-order gradient + L2
		for k := 0; k < m.cfg.NumFactors; k++ {
			sumVK := float32(0)
			for i := 0; i < featLen; i++ {
				if features[i] != 0 {
					sumVK += m.factors[i][k] * features[i]
				}
			}
			for i := 0; i < featLen; i++ {
				if features[i] != 0 {
					// d(score)/d(v_ik) = sumVK * x_i - v_ik * x_i^2
					dFac[i][k] += err*(sumVK*features[i]-m.factors[i][k]*features[i]*features[i]) + l2*m.factors[i][k]
				}
			}
		}
	}

	// Adam update for bias
	m.adamUpdateScalar(&m.bias, &dBias, nil, nil, lr, eps, bc1, bc2, beta1, beta2)

	// Adam update for linear weights
	for i := 0; i < n; i++ {
		if dLin[i] != 0 {
			m.adamUpdateScalar(&m.linear[i], &dLin[i], &m.mLin[i], &m.vLin[i], lr, eps, bc1, bc2, beta1, beta2)
		}
	}

	// Adam update for factors
	for i := 0; i < n; i++ {
		for k := 0; k < m.cfg.NumFactors; k++ {
			if dFac[i][k] != 0 {
				m.adamUpdateScalar(&m.factors[i][k], &dFac[i][k], &m.mFac[i][k], &m.vFac[i][k], lr, eps, bc1, bc2, beta1, beta2)
			}
		}
	}

	return totalLoss / float32(len(batch))
}

// adamUpdateScalar applies a single Adam update step in-place.
func (m *AFM) adamUpdateScalar(param, grad, firstMom, secondMom *float32, lr, eps, bc1, bc2, beta1, beta2 float32) {
	g := *grad
	if firstMom != nil {
		*firstMom = beta1**firstMom + (1-beta1)*g
		*secondMom = beta2**secondMom + (1-beta2)*g*g
		mHat := *firstMom / bc1
		vHat := *secondMom / bc2
		*param -= lr * mHat / (float32(math.Sqrt(float64(vHat))) + eps)
	} else {
		// Scalar bias: no moment tracking, use plain SGD with L2
		*param -= lr * g
	}
}

// AFMRanker implements Ranker using AFM for CTR scoring.
type AFMRanker struct {
	model *AFM
}

// NewAFMRanker creates a ranker wrapping the given trained AFM model.
func NewAFMRanker(model *AFM) *AFMRanker {
	return &AFMRanker{model: model}
}

// Rank scores candidates via AFM CTR prediction and returns topK items.
func (r *AFMRanker) Rank(ctx context.Context, candidates []Candidate, topK int) ([]RankedItem, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	if topK <= 0 || topK > len(candidates) {
		topK = len(candidates)
	}

	results := make([]RankedItem, len(candidates))
	for i, c := range candidates {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		ctr := r.model.Predict(c.Features)
		results[i] = RankedItem{
			ItemID:   c.ItemID,
			Score:    c.Score,
			CTRScore: ctr,
			Source:   c.Source,
		}
	}

	// Partial sort: find top-K by CTRScore descending.
	for i := 0; i < topK; i++ {
		maxIdx := i
		for j := i + 1; j < len(results); j++ {
			if results[j].CTRScore > results[maxIdx].CTRScore ||
				(results[j].CTRScore == results[maxIdx].CTRScore && results[j].Score > results[maxIdx].Score) {
				maxIdx = j
			}
		}
		if maxIdx != i {
			results[i], results[maxIdx] = results[maxIdx], results[i]
		}
	}

	return results[:topK], nil
}
