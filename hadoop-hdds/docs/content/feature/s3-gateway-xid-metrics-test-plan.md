---
title: "S3 Gateway XID Metrics Test Plan"
menu:
   main:
      parent: Features
summary: "Test plan for the per-XID S3 Gateway metrics (SDPOZN-2371)."
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Test Plan: S3 Gateway XID Metrics

- **Document version:** v1.2
- **Date:** 2026-08-04
- **Product:** `S3GatewayXidMetrics` (Apache Ozone, module `hadoop-ozone/s3gateway`)
- **Test target:** the `S3GatewayXidMetrics` class and the monitoring configuration
- **Related documents:** `s3-gateway-xid-metrics-runbook.md`

---

## 1. Objective

Verify the correctness of S3 statistics aggregation by XID, thread safety, percentile calculation, eviction
and cleanup behavior, XID monitoring, the `equals`/`hashCode` contract of the keys, the singleton lifecycle,
and the validity of the monitoring artifacts (dashboard and Prometheus rules).

## 2. Scope (in scope / out of scope)

**In scope:** functionality of `S3GatewayXidMetrics`, unit tests, integration scenarios for monitoring.

**Out of scope:** S3 Gateway JVM metrics, RPC/SCM/OM metrics, and other S3 metrics (`S3GatewayMetrics`).

## 3. Environment

- Maven (offline: the `-o` flag), compiling the module: `mvn -o -pl hadoop-ozone/s3gateway -am test-compile`.
- Running the tests: `mvn -o -pl hadoop-ozone/s3gateway test -Dtest='TestS3GatewayXidMetrics'`.
- JUnit 5 (Jupiter), Mockito, `@ParameterizedTest`, `@NullAndEmptySource`.

> Note: an isolated build of `-pl hadoop-ozone/s3gateway` (without `-am`) fails on the main code because of
> missing symbols. Use the standard build with `-am` (reactor dependencies).

## 4. Test coverage (unit tests)

Test location: `hadoop-ozone/s3gateway/src/test/java/org/apache/hadoop/ozone/s3/metrics/TestS3GatewayXidMetrics.java`.
In total **39 methods / 42 invocations** (37 `@Test` methods + 2 parameterized: one over the codes `200/201/204`
yields 3 invocations, the other with `@NullAndEmptySource` yields 2).

### 4.1 Request aggregation

| ID | Test | Expected result |
|---|---|---|
| A1 | `testBytesGroupedByXidAndRequestType` | Total bytes grouped by `(xid, method)` |
| A2 | `testAverageLatencyGroupedByXid` | Average latency `total/count` per XID |
| A3 | `testRequestCountGroupedByXid` | Number of requests per XID |
| A4 | `testErrorsGroupedByXidAndErrorCode` | Errors grouped by `(xid, error_code)` |

### 4.2 Aggregation edge cases

| ID | Test | Expected result |
|---|---|---|
| B1 | `testSuccessStatusCodesAreNotErrors` (200/201/204) | Successful codes are not written to `error_count` |
| B2 | `testErrorCodeBelow400` | `errorCode < 400` is not an error |
| B3 | `testErrorCodeAt400` | `errorCode == 400` is an error (`ERROR_CODE_THRESHOLD` boundary) |
| B4 | `testXidNormalisedToDefault` (null/empty) | A null/empty XID is normalized to `"none"` |
| B5 | `testNullRequestType` | A `null` `requestType` does not throw NPE |

### 4.3 Percentile correctness

| ID | Test | Expected result |
|---|---|---|
| C1 | `testPercentileSingleSample` | A single sample ⇒ p50=p95=p99=value |
| C2 | `testPercentileEmptySamples` | Empty sample set ⇒ 0.0, no records emitted |
| C3 | `testPercentileLinearInterpolation` | `[1..5]` ⇒ p50=3.0, p95=4.8, p99=4.96 |
| C4 | `testPercentileBoundaries` | Two-point mid-point (p50/p95) |
| C5 | `testPercentileCaching` | Repeated reads reuse the cache |
| C6 | `testPercentileCacheInvalidationOnOverflow` | `removeFirst` rotation invalidates the cache |
| C7 | `testPercentileCacheInvalidationOnClear` | `clearMetrics` resets the cache |

### 4.4 Eviction and cleanup

| ID | Test | Expected result |
|---|---|---|
| D1 | `testEvictionRemovesOldestKey` | When `MAX_KEYS_PER_MAP` is exceeded, an entry is removed |
| D2 | `testMetricsAreDeletedAfterCleanupInterval` | After the interval elapses the metrics are cleared (explicit check that the internal `bytesTotal`/`errorsTotal`/`bytesMetricKeyPool` maps are empty and the collector emits no records) |
| D3 | `testMetricsAreNotDeletedBeforeCleanupInterval` | Before the interval the metrics are kept (explicit guard that the maps are NOT cleared early) |
| D4 | `testClearMetricsCachesPercentiles` | `clearMetrics` empties the maps and the percentile cache |

### 4.5 XID monitoring

| ID | Test | Expected result |
|---|---|---|
| E1 | `testXidMonitoringActiveCount` | `activeXidCount` is emitted when there are active XIDs |
| E2 | `testXidMonitoringClearResetsCount` | `activeXidCount` resets after cleanup |
| E3 | `testXidMonitoringThresholdAlert` | `xidMemoryRatio` appears when `activeXidCount > 5000` |

### 4.6 Keys (`equals`/`hashCode`)

> The nested keys `BytesMetricKey`/`RequestMetricKey` are package-private `static final`, so the tests call them
> directly, without reflection. They verify reflexivity, symmetry, transitivity, consistency, null-safe cases
> (`bothDiff`), `hashCode` consistency, and distribution over all fields.

| ID | Test | Expected result |
|---|---|---|
| F1 | `testBytesMetricKeyEquals` | Full `equals` contract for `BytesMetricKey` |
| F2 | `testBytesMetricKeyHashCode` | Consistent `hashCode` + distribution over fields |
| F3 | `testRequestMetricKeyEquals` | Full `equals` contract for `RequestMetricKey` |
| F4 | `testRequestMetricKeyHashCode` | Consistent `hashCode` + distribution over fields |

### 4.7 Singleton and lifecycle

| ID | Test | Expected result |
|---|---|---|
| G1 | `testGetInstanceReturnsSameInstance` | `getInstance()` returns the same object |
| G2 | `testDCLSingletonThreadSafety` | DCL singleton is thread-safe (a single instance) |
| G3 | `testUnregister` | `unRegister()` is idempotent and throws nothing |

### 4.8 Concurrency

| ID | Test | Expected result |
|---|---|---|
| H1 | `testGetMetricsAndClearMetricsConcurrentAccess` | Concurrent reads and cleanup without exceptions |
| H2 | `testConcurrentRecordRequest` | N×M records under one XID without loss |
| H3 | `testConcurrentRecordRequestDifferentXids` | Unique XIDs without exceptions |
| H4 | `testConcurrentGetMetricsAndRecordRequest` | Read and write without exceptions |
| H5 | `testConcurrentGetMetricsWhileRecording` | Safe reads during recording |
| H6 | `testConcurrentRecordRequestNoDataLoss` | No data loss under contention |

### 4.9 Key interning (GC pressure)

| ID | Test | Expected result |
|---|---|---|
| I1 | `testBytesMetricKeyInternPooling` | Identical `(xid, method)` pairs share one instance |
| I2 | `testBytesMetricKeyInternPoolClearOnClearMetrics` | The pool is cleared together with the metrics |
| I3 | `testBytesMetricKeyInternNoNPE` | `internBytesKey` handles `null` safely |

## 5. Test data

- XID: `"xid-1"`, `"xid-2"`, ... for aggregation; `null`/`""` for normalization checks.
- `errorCode`: `200, 201, 204, 399, 400, 404, 500`.
- `latencyMs`: sample sets for interpolation checks `[1..5]`, `[10,20]`, and a single sample.
- Load cases: samples above `MAX_KEYS_PER_MAP` (eviction), above 5000 XIDs (monitoring threshold).

## 6. Acceptance criteria

- **42/42 green** for the unit test class `TestS3GatewayXidMetrics` (BUILD SUCCESS).
- No `assertTrue(true, ...)` calls.
- **No reflection** in the key tests: the nested `BytesMetricKey`/`RequestMetricKey` are package-private and are
  called directly (including no `setAccessible(true)`).
- Alerts and dashboard are valid (see section 7.2).
- No races in the concurrency tests across repeated runs (`-Dtest=...` ×N).

## 7. Manual verification scenarios

### 7.1 Local run

```bash
mvn -o -pl hadoop-ozone/s3gateway -am test-compile
mvn -o -pl hadoop-ozone/s3gateway test -Dtest='TestS3GatewayXidMetrics'
```

### 7.2 Monitoring

1. Confirm the dashboard `Ozone-S3GatewayXIDMetrics.json` appears in Grafana (provisioning).
2. Confirm Prometheus sees the rules (`rule_files` config) and that `s3g` serves `/prom`.
3. Review the expression queries by prefix `s3_gateway_xid_metrics_` (see the runbook).

## 8. Risks and limitations

- Eviction does not guarantee strict FIFO (`ConcurrentHashMap` — arbitrary iterator order).
- `xidMemoryRatio` is only emitted when `activeXidCount > 5000`.
- Percentiles are limited to the last 2000 samples per XID.
- The concurrency tests rely on the determinism of the mock collector; if runs are flaky, re-run the tests.

## 9. Result

Current state: **42 invocations, 0 failures, 0 errors, 0 skipped, BUILD SUCCESS.**