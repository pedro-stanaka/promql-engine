// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package model

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/stats"
)

type OperatorTelemetry interface {
	AddExecutionTimeTaken(time.Duration)
	ExecutionTimeTaken() time.Duration
	IncrementSamplesAtStep(samples int, step int)
	UpdatePeak(samples int)
	Samples() *stats.QuerySamples
	Name() string
}

func NewTelemetry(name string, enabled bool) OperatorTelemetry {
	if enabled {
		return NewTrackedTelemetry(name)
	}
	return NewNoopTelemetry(name)
}

type NoopTelemetry struct {
	name string
}

func NewNoopTelemetry(name string) *NoopTelemetry {
	return &NoopTelemetry{name: name}
}

func (tm *NoopTelemetry) Name() string { return tm.name }

func (tm *NoopTelemetry) AddExecutionTimeTaken(t time.Duration) {}

func (tm *NoopTelemetry) ExecutionTimeTaken() time.Duration {
	return time.Duration(0)
}

func (tm *NoopTelemetry) IncrementSamplesAtStep(_, _ int) {}

func (tm *NoopTelemetry) UpdatePeak(_ int) {}

func (tm *NoopTelemetry) Samples() *stats.QuerySamples { return stats.NewQuerySamples(false) }

type TrackedTelemetry struct {
	name          string
	ExecutionTime time.Duration

	LoadedSamples *stats.QuerySamples
}

func NewTrackedTelemetry(name string) *TrackedTelemetry {
	return &TrackedTelemetry{name: name, LoadedSamples: stats.NewQuerySamples(false)}
}

func (ti *TrackedTelemetry) Name() string {
	return ti.name
}

func (ti *TrackedTelemetry) AddExecutionTimeTaken(t time.Duration) { ti.ExecutionTime += t }

func (ti *TrackedTelemetry) ExecutionTimeTaken() time.Duration {
	return ti.ExecutionTime
}

func (ti *TrackedTelemetry) IncrementSamplesAtStep(samples, step int) {
	ti.LoadedSamples.IncrementSamplesAtStep(step, int64(samples))
}

func (ti *TrackedTelemetry) UpdatePeak(samples int) {
	ti.LoadedSamples.UpdatePeak(samples)
}

func (ti *TrackedTelemetry) Samples() *stats.QuerySamples { return ti.LoadedSamples }

type ObservableVectorOperator interface {
	VectorOperator
	OperatorTelemetry
}

// VectorOperator performs operations on series in step by step fashion.
type VectorOperator interface {
	// Next yields vectors of samples from all series for one or more execution steps.
	Next(ctx context.Context) ([]StepVector, error)

	// Series returns all series that the operator will process during Next results.
	// The result can be used by upstream operators to allocate output tables and buffers
	// before starting to process samples.
	Series(ctx context.Context) ([]labels.Labels, error)

	// GetPool returns pool of vectors that can be shared across operators.
	GetPool() *VectorPool

	// Explain returns human-readable explanation of the current operator and optional nested operators.
	Explain() (me string, next []VectorOperator)
}
