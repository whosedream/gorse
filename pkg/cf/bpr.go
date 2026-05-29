package cf

import (
	"bytes"
	"encoding/gob"
	"math"
	"math/rand"
)

// BPRParams holds hyperparameters for BPR matrix factorization.
type BPRParams struct {
	NFactors   int     // latent factor dimension
	NEpochs    int     // training epochs
	Lr         float32 // learning rate
	Reg        float32 // L2 regularization
	InitMean   float32 // init mean
	InitStdDev float32 // init stddev
}

// DefaultBPRParams returns the recommended defaults from gorse.
func DefaultBPRParams() BPRParams {
	return BPRParams{
		NFactors:   16,
		NEpochs:    100,
		Lr:         0.05,
		Reg:        0.01,
		InitMean:   0,
		InitStdDev: 0.001,
	}
}

// Interaction stores a single (user, item) interaction.
type Interaction struct {
	UserID string
	ItemID string
}

// BPR implements Bayesian Personalized Ranking matrix factorization.
// It learns user/item latent vectors via SGD over positive/negative pairs.
type BPR struct {
	params BPRParams

	userIndex map[string]int // userID -> latent row
	itemIndex map[string]int // itemID -> latent col
	userIDs  []string        // reverse map
	itemIDs  []string        // reverse map

	UserFactor [][]float32 // [nUsers][nFactors]
	ItemFactor [][]float32 // [nItems][nFactors]

	// userItems maps user index -> set of item indices (for negative sampling)
	userItems map[int]map[int]struct{}
}

// NewBPR creates a BPR model with the given parameters.
func NewBPR(params BPRParams) *BPR {
	if params.NFactors <= 0 {
		params.NFactors = 16
	}
	if params.NEpochs <= 0 {
		params.NEpochs = 100
	}
	if params.Lr <= 0 {
		params.Lr = 0.05
	}
	if params.Reg <= 0 {
		params.Reg = 0.01
	}
	if params.InitStdDev <= 0 {
		params.InitStdDev = 0.001
	}
	return &BPR{
		params:    params,
		userIndex: make(map[string]int),
		itemIndex: make(map[string]int),
		userItems: make(map[int]map[int]struct{}),
	}
}

// Fit trains the BPR model on the given interactions.
func (b *BPR) Fit(interactions []Interaction) {
	// Build index maps
	b.buildIndex(interactions)

	nUsers := len(b.userIDs)
	nItems := len(b.itemIDs)
	nFactors := b.params.NFactors

	// Initialize latent factors with normal distribution
	b.UserFactor = make([][]float32, nUsers)
	b.ItemFactor = make([][]float32, nItems)
	for i := range b.UserFactor {
		b.UserFactor[i] = randNormal(nFactors, b.params.InitMean, b.params.InitStdDev)
	}
	for i := range b.ItemFactor {
		b.ItemFactor[i] = randNormal(nFactors, b.params.InitMean, b.params.InitStdDev)
	}

	// Build user->items mapping for negative sampling
	for _, it := range interactions {
		ui := b.userIndex[it.UserID]
		ii := b.itemIndex[it.ItemID]
		if b.userItems[ui] == nil {
			b.userItems[ui] = make(map[int]struct{})
		}
		b.userItems[ui][ii] = struct{}{}
	}

	// Collect user indices that have interactions
	activeUsers := make([]int, 0, nUsers)
	for ui, items := range b.userItems {
		if len(items) > 0 {
			activeUsers = append(activeUsers, ui)
		}
	}

	// SGD training loop
	rng := rand.New(rand.NewSource(42))
	tmpVec := make([]float32, nFactors)

	for epoch := 0; epoch < b.params.NEpochs; epoch++ {
		for _, ui := range activeUsers {
			items := b.userItems[ui]
			// Sample a positive item
			pi := randomKey(items, rng)

			// Sample a negative item (not in user's items)
			ni := b.sampleNegative(ui, nItems, items, rng)

			// Compute difference: p_u^T * q_i - p_u^T * q_j
			pU := b.UserFactor[ui]
			qI := b.ItemFactor[pi]
			qJ := b.ItemFactor[ni]

			diff := dot(pU, qI) - dot(pU, qJ)
			grad := sigmoid(-diff) // exp(-diff) / (1 + exp(-diff))

			// Update item factor q_i (positive)
			for k := range qI {
				qI[k] += b.params.Lr * (grad*pU[k] - b.params.Reg*qI[k])
			}

			// Update item factor q_j (negative)
			for k := range qJ {
				qJ[k] += b.params.Lr * (-grad*pU[k] - b.params.Reg*qJ[k])
			}

			// Update user factor p_u
			copy(tmpVec, pU)
			for k := range pU {
				pU[k] = tmpVec[k] + b.params.Lr*(grad*(qI[k]-qJ[k])-b.params.Reg*tmpVec[k])
			}
		}
	}
}

// Predict returns the BPR score for a (user, item) pair.
func (b *BPR) Predict(userID, itemID string) float32 {
	ui, ok := b.userIndex[userID]
	if !ok {
		return 0
	}
	ii, ok := b.itemIndex[itemID]
	if !ok {
		return 0
	}
	return dot(b.UserFactor[ui], b.ItemFactor[ii])
}

// UserFactors returns the latent vector for a user.
func (b *BPR) UserFactors(userID string) ([]float32, bool) {
	ui, ok := b.userIndex[userID]
	if !ok {
		return nil, false
	}
	return b.UserFactor[ui], true
}

// ItemIDs returns all known item IDs.
func (b *BPR) ItemIDs() []string {
	return b.itemIDs
}

func (b *BPR) buildIndex(interactions []Interaction) {
	userSet := make(map[string]struct{})
	itemSet := make(map[string]struct{})
	for _, it := range interactions {
		userSet[it.UserID] = struct{}{}
		itemSet[it.ItemID] = struct{}{}
	}
	b.userIDs = make([]string, 0, len(userSet))
	for uid := range userSet {
		b.userIDs = append(b.userIDs, uid)
		b.userIndex[uid] = len(b.userIDs) - 1
	}
	b.itemIDs = make([]string, 0, len(itemSet))
	for iid := range itemSet {
		b.itemIDs = append(b.itemIDs, iid)
		b.itemIndex[iid] = len(b.itemIDs) - 1
	}
}

func (b *BPR) sampleNegative(ui, nItems int, posItems map[int]struct{}, rng *rand.Rand) int {
	// If user interacted with all items, pick any (degenerate case)
	if len(posItems) >= nItems {
		return rng.Intn(nItems)
	}
	for {
		ni := rng.Intn(nItems)
		if _, ok := posItems[ni]; !ok {
			return ni
		}
	}
}

func randomKey(m map[int]struct{}, rng *rand.Rand) int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys[rng.Intn(len(keys))]
}

func dot(a, b []float32) float32 {
	var s float32
	for i := range a {
		s += a[i] * b[i]
	}
	return s
}

func sigmoid(x float32) float32 {
	return 1.0 / (1.0 + float32(math.Exp(-float64(x))))
}

func randNormal(n int, mean, stddev float32) []float32 {
	v := make([]float32, n)
	for i := range v {
		// Box-Muller transform
		u1 := rand.Float32()
		u2 := rand.Float32()
		z := float32(math.Sqrt(-2*math.Log(float64(u1))) * math.Cos(2*math.Pi*float64(u2)))
		v[i] = mean + stddev*z
	}
	return v
}

// gobBPR is the gob-serializable form of BPR.
type gobBPR struct {
	Params    BPRParams
	UserIDs   []string
	ItemIDs   []string
	UserFactor [][]float32
	ItemFactor [][]float32
	UserItems  []map[int]struct{} // serialized per-user positive items
}

// Marshal serializes the BPR model to bytes via gob.
func (b *BPR) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	g := gobBPR{
		Params:     b.params,
		UserIDs:    b.userIDs,
		ItemIDs:    b.itemIDs,
		UserFactor: b.UserFactor,
		ItemFactor: b.ItemFactor,
	}
	g.UserItems = make([]map[int]struct{}, len(b.userIDs))
	for ui, items := range b.userItems {
		g.UserItems[ui] = items
	}
	if err := enc.Encode(g); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Unmarshal deserializes a BPR model from bytes.
func UnmarshalBPR(data []byte) (*BPR, error) {
	var g gobBPR
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&g); err != nil {
		return nil, err
	}
	b := &BPR{
		params:     g.Params,
		userIndex:  make(map[string]int),
		itemIndex:  make(map[string]int),
		UserFactor: g.UserFactor,
		ItemFactor: g.ItemFactor,
		userIDs:    g.UserIDs,
		itemIDs:    g.ItemIDs,
		userItems:  make(map[int]map[int]struct{}),
	}
	for i, uid := range g.UserIDs {
		b.userIndex[uid] = i
	}
	for i, iid := range g.ItemIDs {
		b.itemIndex[iid] = i
	}
	for ui, items := range g.UserItems {
		b.userItems[ui] = items
	}
	return b, nil
}
