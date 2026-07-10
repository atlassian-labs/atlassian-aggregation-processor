// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// This file is copied from github.com/open-telemetry/opentelemetry-collector-contrib/internal/exp/metrics at v0.149.0.

// identity types for metrics and sample streams.
//
// Use the `Of*(T) -> I` functions to obtain a unique, comparable (==) and
// hashable (map key) identity value I for T.
package identity // import "github.com/atlassian-labs/atlassian-aggregation-processor/internal/pmetricutil/identity"
