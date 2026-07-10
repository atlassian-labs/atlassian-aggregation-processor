// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
// Modifications made by Atlassian Pty Ltd.
// Copyright © 2026 Atlassian US, Inc.
// Copyright © 2026 Atlassian Pty Ltd.

package atlassianaggregationprocessor // import "github.com/atlassian-labs/atlassian-aggregation-processor/pkg/processor/atlassianaggregationprocessor"

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/atlassian-labs/atlassian-aggregation-processor/pkg/processor/atlassianaggregationprocessor/internal/metadata"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
)

func TestAggregation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
	}{
		{name: "monotonic_sums_are_passed_through"},
		{name: "delta_sums_double_are_aggregated"},
		{name: "delta_sums_int_are_aggregated"},
		{name: "cumulative_histograms_are_passed_through"},
		{name: "delta_histograms_are_aggregated"},
		{name: "delta_histograms_with_different_buckets_are_dropped"},
		{name: "gauges_are_aggregated"},
		{name: "summaries_are_passed_through"},
		{name: "exp_histograms_are_passed_through"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var config *Config
	for _, tc := range testCases {
		config = &Config{Interval: time.Second}

		t.Run(tc.name, func(t *testing.T) {
			// next stores the results of the filter metric processor
			next := &consumertest.MetricsSink{}

			factory := NewFactory()
			mgp, err := factory.CreateMetrics(
				context.Background(),
				processortest.NewNopSettings(metadata.Type),
				config,
				next,
			)
			require.NoError(t, err)

			dir := filepath.Join("testdata", tc.name)

			md, err := golden.ReadMetrics(filepath.Join(dir, "input.yaml"))
			require.NoError(t, err)

			// Test that ConsumeMetrics works
			err = mgp.ConsumeMetrics(ctx, md)
			require.NoError(t, err)

			require.IsType(t, &aggregationProcessor{}, mgp)
			processor := mgp.(*aggregationProcessor)

			// Pretend we hit the interval timer and call export
			processor.exportMetrics()

			// All the lookup tables should now be empty
			require.Empty(t, processor.rmLookup)
			require.Empty(t, processor.smLookup)
			require.Empty(t, processor.mLookup)
			require.Empty(t, processor.numberLookup)
			require.Empty(t, processor.histogramLookup)
			require.Empty(t, processor.expHistogramLookup)
			require.Empty(t, processor.summaryLookup)

			// Exporting again should return nothing
			processor.exportMetrics()

			// Next should have gotten two data sets:
			// 1. Anything left over from ConsumeMetrics()
			// 2. Anything exported from exportMetrics()
			allMetrics := next.AllMetrics()

			exportData := allMetrics[0]
			nextData := allMetrics[1]

			expectedNextData, err := golden.ReadMetrics(filepath.Join(dir, "next.yaml"))
			require.NoError(t, err)
			require.NoError(t, pmetrictest.CompareMetrics(expectedNextData, nextData))

			expectedExportData, err := golden.ReadMetrics(filepath.Join(dir, "output.yaml"))
			require.NoError(t, err)
			require.NoError(t, pmetrictest.CompareMetrics(expectedExportData, exportData, pmetrictest.IgnoreMetricDataPointsOrder()))
		})
	}
}
