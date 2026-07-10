# Atlassian Aggregation Processor

[![Atlassian license](https://img.shields.io/badge/license-Apache%202.0-blue.svg?style=flat-square)](LICENSE) [![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square)](CONTRIBUTING.md)

This repo contains the source code for the Atlassian Aggregation Processor, an [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) processor that aggregates metrics over a configurable time interval and periodically forwards the result down the pipeline.

The processor is based on the OpenTelemetry Collector Contrib [interval processor](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/intervalprocessor), with a focus on metrics with delta temporality. 

The commits under `/pkg` are synced from a closed-source monorepo. We still welcome contributions that we can sync back upstream — see [CONTRIBUTING.md](CONTRIBUTING.md).

## Usage

Add the processor to a Collector metrics pipeline:

```yaml
processors:
  atlassian_aggregation:
    # Interval at which aggregated metrics are exported. Must be > 0.
    interval: 60s

service:
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [atlassian_aggregation]
      exporters: [otlp]
```

For the supported metric types and the behaviour, please see [./pkg/processor/atlassianaggregationprocessor/README.md](./pkg/processor/atlassianaggregationprocessor/README.md).

## Contributions

Contributions to the Atlassian Aggregation Processor are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

Apache-2.0 License. See [LICENSE](./LICENSE).
