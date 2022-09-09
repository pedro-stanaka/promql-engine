package engine

import (
	"context"
	"sort"

	"github.com/fpetkovski/promql-engine/model"

	"github.com/fpetkovski/promql-engine/executionplan"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/stats"
)

type rangeQuery struct {
	pool *model.VectorPool
	plan executionplan.VectorOperator
}

func newRangeQuery(plan executionplan.VectorOperator, pool *model.VectorPool) promql.Query {
	return &rangeQuery{
		pool: pool,
		plan: plan,
	}
}

func (q *rangeQuery) Exec(ctx context.Context) *promql.Result {
	seriesMap := make([]*promql.Series, 3000)
	for {
		r, err := q.plan.Next(ctx)
		if err != nil {
			return newErrResult(err)
		}
		if r == nil {
			break
		}

		for _, vector := range r {
			for _, sample := range vector.Samples {
				if seriesMap[sample.ID] == nil {
					seriesMap[sample.ID] = &promql.Series{
						Metric: sample.Metric,
						Points: make([]promql.Point, 0),
					}
				}
				seriesMap[sample.ID].Points = append(seriesMap[sample.ID].Points, promql.Point{
					T: vector.T,
					V: sample.V,
				})
			}
			q.plan.GetPool().PutSamples(vector.Samples)
		}
		q.plan.GetPool().PutVectors(r)
	}

	result := make(promql.Matrix, 0, len(seriesMap))
	for _, series := range seriesMap {
		if series != nil {
			result = append(result, *series)
		}
	}

	sort.Sort(result)
	return &promql.Result{
		Value: result,
	}
}

// TODO(fpetkovski): Check if any resources can be released.
func (q *rangeQuery) Close() {}

func (q *rangeQuery) Statement() parser.Statement {
	return nil
}

func (q *rangeQuery) Stats() *stats.Statistics {
	return &stats.Statistics{}
}

func (q *rangeQuery) Cancel() {}

func (q *rangeQuery) String() string { return "" }

func newErrResult(err error) *promql.Result {
	return &promql.Result{Err: err}
}