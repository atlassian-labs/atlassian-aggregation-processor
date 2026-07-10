# Atlassian Aggregation Processor


## Description

The aggregation processor is based on the OpenTelemetry Collector Contrib [interval processor](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/intervalprocessor). It aggregates a subset of metric types over a configurable interval and periodically forwards the result to the next component in the pipeline. Metrics that are not aggregated are passed through unchanged.

This implementation aggregates non-monotonic delta sums and histograms and keeps latest gauge values in the aggregation interval.

The behavior per metric type is:

| Metric type | Behavior |
| ----------- | -------- |
| Gauges | Aggregated (last value wins) |
| Non-monotonic delta sums | Aggregated (values summed) |
| Delta histograms | Aggregated (bucket counts, count, and sum summed; min/max merged) |
| Monotonic sums (any temporality) | Passed through unchanged |
| Non-monotonic cumulative sums | Passed through unchanged |
| Cumulative histograms | Passed through unchanged |
| Summaries | Passed through unchanged |
| Exponential histograms | Passed through unchanged |

Aggregation semantics:

* **Gauges** keep the value of the data point with the most recent timestamp ("last value wins").
* **Non-monotonic delta sums** add data point values together within a stream and keep the most recent timestamp.
* **Delta histograms** add bucket counts, total count, and sum, and merge min/max across data points in a stream. If two data points in the same interval have different explicit bucket boundaries, only the one with the most recent timestamp is kept.

## Configuration

The following settings can be optionally configured:

```yaml
atlassian_aggregation:
  # The interval at which the processor exports the aggregated metrics. Must be > 0.
  [ interval: <duration> | default = 60s ]
```

## Example of metric flows

The following metrics come into the processor to be handled:

| Timestamp | Metric Name  | Type / Temporality       | Attributes        | Value |
| --------- | ------------ | ------------------------ | ----------------- | ----: |
| 0         | test_metric  | Sum, Delta, non-monotonic | labelA: foo       |   4.0 |
| 2         | test_metric  | Sum, Delta, non-monotonic | labelA: bar       |   3.1 |
| 4         | other_metric | Sum, Cumulative, monotonic | fruitType: orange |  77.4 |
| 6         | test_metric  | Sum, Delta, non-monotonic | labelA: foo       |   8.2 |
| 8         | test_metric  | Sum, Delta, non-monotonic | labelA: foo       |  12.8 |
| 10        | test_metric  | Sum, Delta, non-monotonic | labelA: bar       |   6.4 |

The processor immediately passes the following metric to the next component in the pipeline, because monotonic sums are not aggregated:

| Timestamp | Metric Name  | Type / Temporality         | Attributes        | Value |
| --------- | ------------ | -------------------------- | ----------------- | ----: |
| 4         | other_metric | Sum, Cumulative, monotonic | fruitType: orange |  77.4 |

At the next `interval` (60s by default), the processor exports the aggregated `test_metric` data points (values aggregated per attribute set, keeping the latest timestamp):

| Timestamp | Metric Name | Type / Temporality        | Attributes  | Value |
| --------- | ----------- | ------------------------- | ----------- | ----: |
| 8         | test_metric | Sum, Delta, non-monotonic | labelA: foo |  25.0 |
| 10        | test_metric | Sum, Delta, non-monotonic | labelA: bar |   9.5 |

> Note: After exporting, the internal aggregation state is cleared. If no new metrics arrive, the next interval exports nothing.
