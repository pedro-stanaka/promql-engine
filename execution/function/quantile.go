// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package function

import (
	"math"
	"sort"

	"github.com/thanos-io/promql-engine/execution/aggregate"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/util/almost"
)

// smallDeltaTolerance is the threshold for relative deltas between classic
// histogram buckets that will be ignored by the histogram_quantile function
// because they are most likely artifacts of floating point precision issues.
// Testing on 2 sets of real data with bugs arising from small deltas,
// the safe ranges were from:
// - 1e-05 to 1e-15
// - 1e-06 to 1e-15
// Anything to the left of that would cause non-query-sharded data to have
// small deltas ignored (unnecessary and we should avoid this), and anything
// to the right of that would cause query-sharded data to not have its small
// deltas ignored (so the problem won't be fixed).
// For context, query sharding triggers these float precision errors in Mimir.
// To illustrate, with a relative deviation of 1e-12, we need to have 1e12
// observations in the bucket so that the change of one observation is small
// enough to get ignored. With the usual observation rate even of very busy
// services, this will hardly be reached in timeframes that matters for
// monitoring.
const smallDeltaTolerance = 1e-12

type le struct {
	upperBound float64
	count      float64
}

// buckets implements sort.Interface.
type buckets []le

func (b buckets) Len() int           { return len(b) }
func (b buckets) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b buckets) Less(i, j int) bool { return b[i].upperBound < b[j].upperBound }

// bucketQuantile calculates the quantile 'q' based on the given buckets. The
// buckets will be sorted by upperBound by this function (i.e. no sorting
// needed before calling this function). The quantile value is interpolated
// assuming a linear distribution within a bucket. However, if the quantile
// falls into the highest bucket, the upper bound of the 2nd highest bucket is
// returned. A natural lower bound of 0 is assumed if the upper bound of the
// lowest bucket is greater 0. In that case, interpolation in the lowest bucket
// happens linearly between 0 and the upper bound of the lowest bucket.
// However, if the lowest bucket has an upper bound less or equal 0, this upper
// bound is returned if the quantile falls into the lowest bucket.
//
// There are a number of special cases (once we have a way to report errors
// happening during evaluations of AST functions, we should report those
// explicitly):
//
// If 'buckets' has 0 observations, NaN is returned.
//
// If 'buckets' has fewer than 2 elements, NaN is returned.
//
// If the highest bucket is not +Inf, NaN is returned.
//
// If q==NaN, NaN is returned.
//
// If q<0, -Inf is returned.
//
// If q>1, +Inf is returned.
func bucketQuantile(q float64, buckets buckets) (float64, bool, bool) {
	if math.IsNaN(q) {
		return math.NaN(), false, false
	}
	if q < 0 {
		return math.Inf(-1), false, false
	}
	if q > 1 {
		return math.Inf(+1), false, false
	}
	sort.Sort(buckets)
	if !math.IsInf(buckets[len(buckets)-1].upperBound, +1) {
		return math.NaN(), false, false
	}

	buckets = coalesceBuckets(buckets)
	forcedMonotonic, fixedPrecision := ensureMonotonicAndIgnoreSmallDeltas(buckets, smallDeltaTolerance)

	if len(buckets) < 2 {
		return math.NaN(), false, false
	}
	observations := buckets[len(buckets)-1].count
	if observations == 0 {
		return math.NaN(), false, false
	}
	rank := q * observations
	b := sort.Search(len(buckets)-1, func(i int) bool { return buckets[i].count >= rank })

	if b == len(buckets)-1 {
		return buckets[len(buckets)-2].upperBound, forcedMonotonic, fixedPrecision
	}
	if b == 0 && buckets[0].upperBound <= 0 {
		return buckets[0].upperBound, forcedMonotonic, fixedPrecision
	}
	var (
		bucketStart float64
		bucketEnd   = buckets[b].upperBound
		count       = buckets[b].count
	)
	if b > 0 {
		bucketStart = buckets[b-1].upperBound
		count -= buckets[b-1].count
		rank -= buckets[b-1].count
	}
	return bucketStart + (bucketEnd-bucketStart)*(rank/count), forcedMonotonic, fixedPrecision
}

// coalesceBuckets merges buckets with the same upper bound.
//
// The input buckets must be sorted.
func coalesceBuckets(buckets buckets) buckets {
	last := buckets[0]
	i := 0
	for _, b := range buckets[1:] {
		if b.upperBound == last.upperBound {
			last.count += b.count
		} else {
			buckets[i] = last
			last = b
			i++
		}
	}
	buckets[i] = last
	return buckets[:i+1]
}

// The assumption that bucket counts increase monotonically with increasing
// upperBound may be violated during:
//
//   - Circumstances where data is already inconsistent at the target's side.
//   - Ingestion via the remote write receiver that Prometheus implements.
//   - Optimisation of query execution where precision is sacrificed for other
//     benefits, not by Prometheus but by systems built on top of it.
//   - Circumstances where floating point precision errors accumulate.
//
// Monotonicity is usually guaranteed because if a bucket with upper bound
// u1 has count c1, then any bucket with a higher upper bound u > u1 must
// have counted all c1 observations and perhaps more, so that c >= c1.
//
// bucketQuantile depends on that monotonicity to do a binary search for the
// bucket with the φ-quantile count, so breaking the monotonicity
// guarantee causes bucketQuantile() to return undefined (nonsense) results.
//
// As a somewhat hacky solution, we first silently ignore any numerically
// insignificant (relative delta below the requested tolerance and likely to
// be from floating point precision errors) differences between successive
// buckets regardless of the direction. Then we calculate the "envelope" of
// the histogram buckets, essentially removing any decreases in the count
// between successive buckets.
//
// We return a bool to indicate if this monotonicity was forced or not, and
// another bool to indicate if small deltas were ignored or not.
func ensureMonotonicAndIgnoreSmallDeltas(buckets buckets, tolerance float64) (bool, bool) {
	var forcedMonotonic, fixedPrecision bool
	prev := buckets[0].count
	for i := 1; i < len(buckets); i++ {
		curr := buckets[i].count // Assumed always positive.
		if curr == prev {
			// No correction needed if the counts are identical between buckets.
			continue
		}
		if almost.Equal(prev, curr, tolerance) {
			// Silently correct numerically insignificant differences from floating
			// point precision errors, regardless of direction.
			// Do not update the 'prev' value as we are ignoring the difference.
			buckets[i].count = prev
			fixedPrecision = true
			continue
		}
		if curr < prev {
			// Force monotonicity by removing any decreases regardless of magnitude.
			// Do not update the 'prev' value as we are ignoring the decrease.
			buckets[i].count = prev
			forcedMonotonic = true
			continue
		}
		prev = curr
	}
	return forcedMonotonic, fixedPrecision
}

// TODO: import from prometheus once exported there.
func histogramStdDev(h *histogram.FloatHistogram) float64 {
	mean := h.Sum / h.Count
	var variance, cVariance float64
	it := h.AllBucketIterator()
	for it.Next() {
		bucket := it.At()
		if bucket.Count == 0 {
			continue
		}
		var val float64
		if bucket.Lower <= 0 && 0 <= bucket.Upper {
			val = 0
		} else {
			val = math.Sqrt(bucket.Upper * bucket.Lower)
			if bucket.Upper < 0 {
				val = -val
			}
		}
		delta := val - mean
		variance, cVariance = aggregate.KahanSumInc(bucket.Count*delta*delta, variance, cVariance)
	}
	variance += cVariance
	variance /= h.Count
	return math.Sqrt(variance)
}

// TODO: import from prometheus once exported there.
func histogramStdVar(h *histogram.FloatHistogram) float64 {
	mean := h.Sum / h.Count
	var variance, cVariance float64
	it := h.AllBucketIterator()
	for it.Next() {
		bucket := it.At()
		if bucket.Count == 0 {
			continue
		}
		var val float64
		if bucket.Lower <= 0 && 0 <= bucket.Upper {
			val = 0
		} else {
			val = math.Sqrt(bucket.Upper * bucket.Lower)
			if bucket.Upper < 0 {
				val = -val
			}
		}
		delta := val - mean
		variance, cVariance = aggregate.KahanSumInc(bucket.Count*delta*delta, variance, cVariance)
	}
	variance += cVariance
	variance /= h.Count
	return variance
}
