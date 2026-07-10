// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
// Modifications made by Atlassian Pty Ltd.
// Copyright © 2026 Atlassian US, Inc.
// Copyright © 2026 Atlassian Pty Ltd.

package atlassianaggregationprocessor // import "github.com/atlassian-labs/atlassian-aggregation-processor/pkg/processor/atlassianaggregationprocessor"

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"

	"github.com/atlassian-labs/atlassian-aggregation-processor/pkg/processor/atlassianaggregationprocessor/internal/metadata"
)

// NewFactory returns a new factory for the Metrics Generation processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, metadata.MetricsStability))
}

func createDefaultConfig() component.Config {
	return &Config{
		Interval: 60 * time.Second,
	}
}

func createMetricsProcessor(_ context.Context, set processor.Settings, cfg component.Config, nextConsumer consumer.Metrics) (processor.Metrics, error) {
	processorConfig, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("configuration parsing error")
	}

	return newProcessor(processorConfig, set.Logger, nextConsumer), nil
}
