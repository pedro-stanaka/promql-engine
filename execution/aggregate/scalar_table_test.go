// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache 2.0.

package aggregate

import (
	"fmt"
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/promql-engine/execution/model"
)

func TestScalarSumTables(t *testing.T) {
	inputs := []uint64{0, 0, 1}
	outputs := []*model.Series{{ID: 0}, {ID: 1}}
	tables, err := newScalarTables(3, inputs, outputs, parser.SUM)
	require.NoError(t, err)

	original := make([]*scalarTable, len(tables))
	for i := range original {
		original[i], err = newScalarTable(inputs, outputs, parser.SUM)
		require.NoError(t, err)
	}
	compare := func() {
		for i, table := range tables {
			optimized := table.(*scalarTable)
			require.Equal(t, original[i].timestamp(), optimized.timestamp())
			for j, acc := range optimized.accumulators {
				require.Equal(t, original[i].accumulators[j].ValueType(), acc.ValueType())
				require.Equal(t, original[i].accumulators[j].Warnings(), acc.Warnings())
				wantFloat, wantHistogram := original[i].accumulators[j].Value()
				gotFloat, gotHistogram := acc.Value()
				require.Equal(t, wantFloat, gotFloat)
				require.Equal(t, wantHistogram, gotHistogram)
			}
		}
	}
	compare()

	floatStep := model.StepVector{T: 1, SampleIDs: []uint64{0, 1, 2}, Samples: []float64{1, 2, 4}}
	require.NoError(t, tables[0].aggregate(floatStep))
	require.NoError(t, original[0].aggregate(floatStep))
	compare()

	h := &histogram.FloatHistogram{Schema: 0, ZeroThreshold: 0.001, Count: 1, ZeroCount: 1, Sum: 2}
	mixedStep := model.StepVector{T: 2, SampleIDs: []uint64{0}, Samples: []float64{3}, HistogramIDs: []uint64{0, 1}, Histograms: []*histogram.FloatHistogram{h, h}}
	require.NoError(t, tables[1].aggregate(mixedStep))
	require.NoError(t, original[1].aggregate(mixedStep))
	compare()

	thirdStep := model.StepVector{T: 3, HistogramIDs: []uint64{2}, Histograms: []*histogram.FloatHistogram{h}}
	require.NoError(t, tables[2].aggregate(thirdStep))
	require.NoError(t, original[2].aggregate(thirdStep))
	compare()

	tables[0].reset(0)
	original[0].reset(0)
	compare() // Resetting one step must not touch the other steps' accumulators.
	require.NoError(t, tables[0].aggregate(floatStep))
	require.NoError(t, original[0].aggregate(floatStep))
	compare()
}

var scalarSumTablesSink []aggregateTable

func BenchmarkScalarSumTables(b *testing.B) {
	for _, n := range []int{128, 1024, 8192} {
		b.Run(fmt.Sprintf("groups=%d", n), func(b *testing.B) {
			outputs := make([]*model.Series, n)
			inputs := make([]uint64, n)
			for i := range outputs {
				outputs[i] = &model.Series{ID: uint64(i)}
				inputs[i] = uint64(i)
			}
			b.ReportAllocs()
			for b.Loop() {
				tables, err := newScalarTables(16, inputs, outputs, parser.SUM)
				if err != nil {
					b.Fatal(err)
				}
				scalarSumTablesSink = tables
			}
		})
	}
}

func TestScalarSumTablesEmpty(t *testing.T) {
	for _, steps := range []int{0, 1, 16} {
		t.Run(fmt.Sprint(steps), func(t *testing.T) {
			tables, err := newScalarTables(steps, nil, nil, parser.SUM)
			require.NoError(t, err)
			require.Len(t, tables, steps)
			for _, table := range tables {
				require.Equal(t, int64(math.MinInt64), table.timestamp())
			}
		})
	}
}
