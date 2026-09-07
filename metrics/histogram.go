package metrics

import (
	"math"
	"math/bits"
	"sort"
)

// histogram is a sparse HDR-style histogram: exact below histSubBuckets, then
// histSubBuckets linear bins per power-of-two octave (~6% resolution). It is
// what keeps a series O(1) in memory however many samples it sees — and why
// the runner marks percentiles derived from it "hist" rather than "exact".
type histogram struct {
	counts map[int]uint64
	total  uint64
}

const (
	histSubBucketBits = 4
	histSubBuckets    = 1 << histSubBucketBits
)

func (h *histogram) add(v int64) {
	if v < 0 {
		v = 0
	}
	if h.counts == nil {
		h.counts = make(map[int]uint64)
	}
	h.counts[bucketIndex(v)]++
	h.total++
}

func (h *histogram) percentile(p float64) int64 {
	if h.total == 0 {
		return 0
	}
	target := uint64(math.Ceil(p * float64(h.total)))
	if target == 0 {
		target = 1
	}
	idxs := make([]int, 0, len(h.counts))
	for i := range h.counts {
		idxs = append(idxs, i)
	}
	sort.Ints(idxs)
	var cum uint64
	for _, i := range idxs {
		cum += h.counts[i]
		if cum >= target {
			return bucketLowerBound(i)
		}
	}
	return bucketLowerBound(idxs[len(idxs)-1])
}

func bucketIndex(v int64) int {
	if v < histSubBuckets {
		return int(v)
	}
	e := bits.Len64(uint64(v)) - 1 // floor(log2 v)
	sub := int((v >> uint(e-histSubBucketBits)) - histSubBuckets)
	return (e-histSubBucketBits+1)*histSubBuckets + sub
}

func bucketLowerBound(idx int) int64 {
	if idx < histSubBuckets {
		return int64(idx)
	}
	octave := idx/histSubBuckets - 1
	sub := idx % histSubBuckets
	return (int64(histSubBuckets) + int64(sub)) << uint(octave)
}
