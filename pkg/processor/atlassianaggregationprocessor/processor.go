// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
// Modifications made by Atlassian Pty Ltd.
// Copyright © 2026 Atlassian US, Inc.
// Copyright © 2026 Atlassian Pty Ltd.

package atlassianaggregationprocessor // import "github.com/atlassian-labs/atlassian-aggregation-processor/pkg/processor/atlassianaggregationprocessor"

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/atlassian-labs/atlassian-aggregation-processor/internal/pmetricutil/identity"
)

var _ processor.Metrics = (*aggregationProcessor)(nil)

type aggregationProcessor struct {
	ctx    context.Context
	cancel context.CancelFunc
	logger *zap.Logger

	stateLock sync.Mutex

	md                 pmetric.Metrics
	rmLookup           map[identity.Resource]pmetric.ResourceMetrics
	smLookup           map[identity.Scope]pmetric.ScopeMetrics
	mLookup            map[identity.Metric]pmetric.Metric
	numberLookup       map[identity.Stream]pmetric.NumberDataPoint
	histogramLookup    map[identity.Stream]pmetric.HistogramDataPoint
	expHistogramLookup map[identity.Stream]pmetric.ExponentialHistogramDataPoint
	summaryLookup      map[identity.Stream]pmetric.SummaryDataPoint

	config *Config

	nextConsumer consumer.Metrics
}

func newProcessor(config *Config, log *zap.Logger, nextConsumer consumer.Metrics) *aggregationProcessor {
	ctx, cancel := context.WithCancel(context.Background())

	return &aggregationProcessor{
		ctx:    ctx,
		cancel: cancel,
		logger: log,

		stateLock: sync.Mutex{},

		md:                 pmetric.NewMetrics(),
		rmLookup:           map[identity.Resource]pmetric.ResourceMetrics{},
		smLookup:           map[identity.Scope]pmetric.ScopeMetrics{},
		mLookup:            map[identity.Metric]pmetric.Metric{},
		numberLookup:       map[identity.Stream]pmetric.NumberDataPoint{},
		histogramLookup:    map[identity.Stream]pmetric.HistogramDataPoint{},
		expHistogramLookup: map[identity.Stream]pmetric.ExponentialHistogramDataPoint{},
		summaryLookup:      map[identity.Stream]pmetric.SummaryDataPoint{},

		config: config,

		nextConsumer: nextConsumer,
	}
}

func (p *aggregationProcessor) Start(_ context.Context, _ component.Host) error {
	exportTicker := time.NewTicker(p.config.Interval)
	go func() {
		for {
			select {
			case <-p.ctx.Done():
				exportTicker.Stop()
				return
			case <-exportTicker.C:
				p.exportMetrics()
			}
		}
	}()

	return nil
}

func (p *aggregationProcessor) Shutdown(_ context.Context) error {
	p.cancel()
	return nil
}

func (*aggregationProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *aggregationProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	var errs error

	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rm.ScopeMetrics().RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			sm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				switch m.Type() {
				case pmetric.MetricTypeSummary:
					return false
				case pmetric.MetricTypeGauge:
					mClone, metricID := p.getOrCloneMetric(rm, sm, m)
					aggregateGauge(m.Gauge().DataPoints(), mClone.Gauge().DataPoints(), metricID, p.numberLookup)
					return true
				case pmetric.MetricTypeSum:
					// Check if we care about this value
					sum := m.Sum()
					// We only care about non-monotonic sums that are not cumulative, monotonic/cumulative sums are passed through
					if sum.IsMonotonic() || sum.AggregationTemporality() == pmetric.AggregationTemporalityCumulative {
						return false
					}

					mClone, metricID := p.getOrCloneMetric(rm, sm, m)
					cloneSum := mClone.Sum()

					aggregateSum(sum.DataPoints(), cloneSum.DataPoints(), metricID, p.numberLookup)
					return true
				case pmetric.MetricTypeHistogram:
					histogram := m.Histogram()

					if histogram.AggregationTemporality() != pmetric.AggregationTemporalityDelta {
						return false
					}

					mClone, metricID := p.getOrCloneMetric(rm, sm, m)
					cloneHistogram := mClone.Histogram()

					aggregateHistogram(histogram.DataPoints(), cloneHistogram.DataPoints(), metricID, p.histogramLookup)
					return true
				case pmetric.MetricTypeExponentialHistogram:
					return false
				default:
					errs = errors.Join(fmt.Errorf("invalid MetricType %d", m.Type()))
					return false
				}
			})
			return sm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})

	if md.ResourceMetrics().Len() == 0 {
		return nil
	}

	if err := p.nextConsumer.ConsumeMetrics(ctx, md); err != nil {
		errs = errors.Join(errs, err)
	}

	return errs
}

func aggregateGauge(dataPoints, mCloneDataPoints pmetric.NumberDataPointSlice, metricID identity.Metric, gaugeLookup map[identity.Stream]pmetric.NumberDataPoint) {
	for i := 0; i < dataPoints.Len(); i++ {
		dp := dataPoints.At(i)

		streamID := identity.OfStream(metricID, dp)
		existingDP, ok := gaugeLookup[streamID]
		if !ok {
			dpClone := mCloneDataPoints.AppendEmpty()
			dp.MoveTo(dpClone)
			gaugeLookup[streamID] = dpClone
			continue
		}

		// Last value wins.
		if dp.Timestamp() > existingDP.Timestamp() {
			switch dp.ValueType() {
			case pmetric.NumberDataPointValueTypeInt:
				existingDP.SetIntValue(dp.IntValue())
			case pmetric.NumberDataPointValueTypeDouble:
				existingDP.SetDoubleValue(dp.DoubleValue())
			}
			existingDP.SetTimestamp(dp.Timestamp())
			existingDP.SetFlags(dp.Flags())
			dp.Exemplars().CopyTo(existingDP.Exemplars())
		}
	}
}

func aggregateHistogram(dataPoints, mCloneDataPoints pmetric.HistogramDataPointSlice, metricID identity.Metric, histogramLookup map[identity.Stream]pmetric.HistogramDataPoint) {
	for i := 0; i < dataPoints.Len(); i++ {
		dp := dataPoints.At(i)

		streamID := identity.OfStream(metricID, dp)
		existingDP, ok := histogramLookup[streamID]
		if !ok {
			dpClone := mCloneDataPoints.AppendEmpty()
			dp.CopyTo(dpClone)
			histogramLookup[streamID] = dpClone
			continue
		}

		// Check if bucket boundaries are the same, if not, keep the one with the latest timestamp
		if !existingDP.ExplicitBounds().Equal(dp.ExplicitBounds()) {
			if dp.Timestamp() > existingDP.Timestamp() {
				dp.CopyTo(existingDP)
			}
			continue
		}

		for j := 0; j < dp.BucketCounts().Len(); j++ {
			existingDP.BucketCounts().SetAt(j, existingDP.BucketCounts().At(j)+dp.BucketCounts().At(j))
		}

		existingDP.SetCount(existingDP.Count() + dp.Count())
		if dp.HasSum() {
			if existingDP.HasSum() {
				existingDP.SetSum(dp.Sum() + existingDP.Sum())
			} else {
				existingDP.SetSum(dp.Sum())
			}
		}
		if dp.HasMin() {
			if !existingDP.HasMin() || dp.Min() < existingDP.Min() {
				existingDP.SetMin(dp.Min())
			}
		}
		if dp.HasMax() {
			if !existingDP.HasMax() || dp.Max() > existingDP.Max() {
				existingDP.SetMax(dp.Max())
			}
		}

		// Check timestamps and keep the latest timestamp and earliest start time
		if dp.Timestamp() > existingDP.Timestamp() {
			existingDP.SetTimestamp(dp.Timestamp())
		}
		if dp.StartTimestamp() < existingDP.StartTimestamp() {
			existingDP.SetStartTimestamp(dp.StartTimestamp())
		}
	}
}

func (p *aggregationProcessor) exportMetrics() {
	md := func() pmetric.Metrics {
		p.stateLock.Lock()
		defer p.stateLock.Unlock()

		// ConsumeMetrics() has prepared our own pmetric.Metrics instance ready for us to use
		// Take it and clear replace it with a new empty one
		out := p.md
		p.md = pmetric.NewMetrics()

		// Clear all the lookup references
		clear(p.rmLookup)
		clear(p.smLookup)
		clear(p.mLookup)
		clear(p.numberLookup)
		clear(p.histogramLookup)
		clear(p.expHistogramLookup)
		clear(p.summaryLookup)

		return out
	}()

	if err := p.nextConsumer.ConsumeMetrics(p.ctx, md); err != nil {
		p.logger.Error("Metrics export failed", zap.Error(err))
	}
}

func aggregateSum(dataPoints, mCloneDataPoints pmetric.NumberDataPointSlice, metricID identity.Metric, sumLookup map[identity.Stream]pmetric.NumberDataPoint) {
	for i := 0; i < dataPoints.Len(); i++ {
		dp := dataPoints.At(i)

		streamID := identity.OfStream(metricID, dp)
		existingDP, ok := sumLookup[streamID]
		// if does not exist, clone create a new one
		if !ok {
			dpClone := mCloneDataPoints.AppendEmpty()
			dp.CopyTo(dpClone)
			sumLookup[streamID] = dpClone
			continue
		}

		switch existingDP.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			existingDP.SetIntValue(
				existingDP.IntValue() + dp.IntValue(),
			)

		case pmetric.NumberDataPointValueTypeDouble:
			existingDP.SetDoubleValue(
				existingDP.DoubleValue() + dp.DoubleValue(),
			)
		}

		// Update the timestamp to the latest one
		if dp.Timestamp() > existingDP.Timestamp() {
			existingDP.SetTimestamp(dp.Timestamp())
		}
	}
}

func (p *aggregationProcessor) getOrCloneMetric(rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, m pmetric.Metric) (pmetric.Metric, identity.Metric) {
	// Find the ResourceMetrics
	resID := identity.OfResource(rm.Resource())
	rmClone, ok := p.rmLookup[resID]
	if !ok {
		// We need to clone it *without* the ScopeMetricsSlice data
		rmClone = p.md.ResourceMetrics().AppendEmpty()
		rm.Resource().CopyTo(rmClone.Resource())
		rmClone.SetSchemaUrl(rm.SchemaUrl())
		p.rmLookup[resID] = rmClone
	}

	// Find the ScopeMetrics
	scopeID := identity.OfScope(resID, sm.Scope())
	smClone, ok := p.smLookup[scopeID]
	if !ok {
		// We need to clone it *without* the MetricSlice data
		smClone = rmClone.ScopeMetrics().AppendEmpty()
		sm.Scope().CopyTo(smClone.Scope())
		smClone.SetSchemaUrl(sm.SchemaUrl())
		p.smLookup[scopeID] = smClone
	}

	// Find the Metric
	metricID := identity.OfMetric(scopeID, m)
	mClone, ok := p.mLookup[metricID]
	if !ok {
		// We need to clone it *without* the datapoint data
		mClone = smClone.Metrics().AppendEmpty()
		mClone.SetName(m.Name())
		mClone.SetDescription(m.Description())
		mClone.SetUnit(m.Unit())

		switch m.Type() {
		case pmetric.MetricTypeGauge:
			mClone.SetEmptyGauge()
		case pmetric.MetricTypeSummary:
			mClone.SetEmptySummary()
		case pmetric.MetricTypeSum:
			src := m.Sum()

			dest := mClone.SetEmptySum()
			dest.SetAggregationTemporality(src.AggregationTemporality())
			dest.SetIsMonotonic(src.IsMonotonic())
		case pmetric.MetricTypeHistogram:
			src := m.Histogram()

			dest := mClone.SetEmptyHistogram()
			dest.SetAggregationTemporality(src.AggregationTemporality())
		case pmetric.MetricTypeExponentialHistogram:
			src := m.ExponentialHistogram()

			dest := mClone.SetEmptyExponentialHistogram()
			dest.SetAggregationTemporality(src.AggregationTemporality())
		}

		p.mLookup[metricID] = mClone
	}

	return mClone, metricID
}
