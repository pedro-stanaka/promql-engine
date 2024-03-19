// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/promql-engine/engine"
)

func TestQueryExplain(t *testing.T) {
	t.Parallel()
	opts := promql.EngineOpts{Timeout: 1 * time.Hour}
	series := storage.MockSeries(
		[]int64{240, 270, 300, 600, 630, 660},
		[]float64{1, 2, 3, 4, 5, 6},
		[]string{labels.MetricName, "foo"},
	)

	start := time.Unix(0, 0)
	end := time.Unix(1000, 0)

	// Calculate concurrencyOperators according to max available CPUs.
	totalOperators := runtime.GOMAXPROCS(0) / 2
	var concurrencyOperators []engine.ExplainOutputNode
	for i := 0; i < totalOperators; i++ {
		concurrencyOperators = append(concurrencyOperators, engine.ExplainOutputNode{
			OperatorName: "[concurrent(buff=2)]", Children: []engine.ExplainOutputNode{
				{OperatorName: fmt.Sprintf("[vectorSelector] {[__name__=\"foo\"]} %d mod %d", i, totalOperators)},
			},
		})
	}

	for _, tc := range []struct {
		query    string
		expected *engine.ExplainOutputNode
	}{
		{
			query:    "time()",
			expected: &engine.ExplainOutputNode{OperatorName: "[noArgFunction] time()"},
		},
		{
			query:    "foo",
			expected: &engine.ExplainOutputNode{OperatorName: "[coalesce]", Children: concurrencyOperators},
		},
		{
			query: "sum(foo) by (job)",
			expected: &engine.ExplainOutputNode{OperatorName: "[concurrent(buff=2)]", Children: []engine.ExplainOutputNode{
				{OperatorName: "[aggregate] sum by ([job])", Children: []engine.ExplainOutputNode{
					{OperatorName: "[coalesce]", Children: concurrencyOperators},
				},
				},
			},
			},
		},
	} {
		{
			t.Run(tc.query, func(t *testing.T) {
				ng := engine.New(engine.Opts{EngineOpts: opts})
				ctx := context.Background()

				var (
					query promql.Query
					err   error
				)

				query, err = ng.NewInstantQuery(ctx, storageWithSeries(series), nil, tc.query, start)
				testutil.Ok(t, err)

				explainableQuery := query.(engine.ExplainableQuery)
				testutil.Equals(t, tc.expected, explainableQuery.Explain())

				query, err = ng.NewRangeQuery(ctx, storageWithSeries(series), nil, tc.query, start, end, 30*time.Second)
				testutil.Ok(t, err)

				explainableQuery = query.(engine.ExplainableQuery)
				testutil.Equals(t, tc.expected, explainableQuery.Explain())
			})
		}
	}
}

func assertExecutionTimeNonZero(t *testing.T, got *engine.AnalyzeOutputNode) bool {
	if got != nil {
		if got.OperatorTelemetry.ExecutionTimeTaken() <= 0 {
			t.Errorf("expected non-zero ExecutionTime for Operator, got %s ", got.OperatorTelemetry.ExecutionTimeTaken())
			return false
		}
		for i := range got.Children {
			child := got.Children[i]
			return got.OperatorTelemetry.ExecutionTimeTaken() > 0 && assertExecutionTimeNonZero(t, &child)
		}
	}
	return true
}

func TestQueryAnalyze(t *testing.T) {
	opts := promql.EngineOpts{Timeout: 1 * time.Hour}
	series := storage.MockSeries(
		[]int64{240, 270, 300, 600, 630, 660},
		[]float64{1, 2, 3, 4, 5, 6},
		[]string{labels.MetricName, "foo"},
	)

	start := time.Unix(0, 0)
	end := time.Unix(1000, 0)

	for _, tc := range []struct {
		query string
	}{
		{
			query: "foo",
		},
		{
			query: "time()",
		},
		{
			query: "sum(foo) by (job)",
		},
		{
			query: "rate(http_requests_total[30s]) > bool 0",
		},
	} {
		tc := tc
		{
			t.Run(tc.query, func(t *testing.T) {
				t.Parallel()
				ng := engine.New(engine.Opts{EngineOpts: opts, EnableAnalysis: true})
				ctx := context.Background()

				var (
					query promql.Query
					err   error
				)

				query, err = ng.NewInstantQuery(ctx, storageWithSeries(series), nil, tc.query, start)
				testutil.Ok(t, err)

				queryResults := query.Exec(context.Background())
				testutil.Ok(t, queryResults.Err)

				explainableQuery := query.(engine.ExplainableQuery)

				testutil.Assert(t, assertExecutionTimeNonZero(t, explainableQuery.Analyze()))

				query, err = ng.NewRangeQuery(ctx, storageWithSeries(series), nil, tc.query, start, end, 30*time.Second)
				testutil.Ok(t, err)

				queryResults = query.Exec(context.Background())
				testutil.Ok(t, queryResults.Err)

				explainableQuery = query.(engine.ExplainableQuery)
				testutil.Assert(t, assertExecutionTimeNonZero(t, explainableQuery.Analyze()))
			})
		}
	}
}

func TestAnalyzeOutputNode_Samples(t *testing.T) {
	t.Parallel()
	ng := engine.New(engine.Opts{EngineOpts: promql.EngineOpts{Timeout: 1 * time.Hour}, EnableAnalysis: true})
	ctx := context.Background()

	load := `load 30s
				http_requests_total{pod="nginx-1"} 1+1x10
				http_requests_total{pod="nginx-2"} 1+2x14`

	tstorage := promql.LoadedStorage(t, load)
	defer tstorage.Close()

	query, err := ng.NewRangeQuery(
		ctx,
		tstorage,
		promql.NewPrometheusQueryOpts(false, 0),
		"sum(rate(http_requests_total[1m])) by (pod)",
		time.Unix(0, 0),
		time.Unix(2*60*60, 0),
		60*time.Second,
	)
	testutil.Ok(t, err)

	queryResults := query.Exec(context.Background())
	testutil.Ok(t, queryResults.Err)

	explainableQuery := query.(engine.ExplainableQuery)
	analyzeOutput := explainableQuery.Analyze()
	require.Greater(t, analyzeOutput.PeakSamples(), int64(0))
	require.Greater(t, analyzeOutput.TotalSamples(), int64(0))
}
