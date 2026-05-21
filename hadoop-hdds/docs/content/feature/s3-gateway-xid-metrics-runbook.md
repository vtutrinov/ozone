---
title: "S3 Gateway XID Metrics Runbook"
menu:
   main:
      parent: Features
summary: "Operating guide for the per-XID S3 Gateway metrics (SDPOZN-2371): dashboards, alerts and troubleshooting."
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

# Operating Guide for S3 Gateway XID Metrics

- **Document version:** v1.1
- **Date:** 2026-08-04
- **Scope:** monitoring and operations of the `S3GatewayXidMetrics` metrics (Apache Ozone)
- **Related files:** `Ozone-S3GatewayXIDMetrics.json`, `s3-gateway-xid-alerts.yml`

---

## 1. Purpose

S3 Gateway can accept an XID header — the session identifier of an external client. For each XID the
service accumulates aggregated statistics: bytes transferred, request count, error count, and response
latency (including the p50/p95/p99 percentiles).

`S3GatewayXidMetrics` is a thread-safe singleton that implements `MetricsSource`. It registers with
`OzoneMetricsSystem` the first time `getInstance()` is called.

This document describes which metrics are collected, which Prometheus metric names to use in dashboards and
alerts, which thresholds are critical, and what to do during an incident.

---

## 2. Mapping Metrics2 names to Prometheus

Hadoop Metrics2 metric names are translated to Prometheus using the rule
`<record_source_name_in_snake_case>_<metric_name_in_snake_case>`.

For this source, `SOURCE_NAME = "S3GatewayXidMetrics"` maps to the Prometheus prefix `s3_gateway_xid_metrics_`.

### 2.1 Metric reference table

| Metrics2 name | Prometheus name | Type | Labels | Emitted when |
|---|---|---|---|---|
| `sum_bytes` | `s3_gateway_xid_metrics_sum_bytes` | counter | `xid`, `method` | data is present |
| `count_requests` | `s3_gateway_xid_metrics_count_requests` | counter | `xid` | data is present |
| `error_count` | `s3_gateway_xid_metrics_error_count` | counter | `xid`, `error_code` | `errorCode >= 400` |
| `average_latency` | `s3_gateway_xid_metrics_average_latency` | gauge | `xid` | data is present |
| `request_latency_ms_p50` | `s3_gateway_xid_metrics_request_latency_ms_p50` | gauge | `xid` | data is present |
| `request_latency_ms_p95` | `s3_gateway_xid_metrics_request_latency_ms_p95` | gauge | `xid` | data is present |
| `request_latency_ms_p99` | `s3_gateway_xid_metrics_request_latency_ms_p99` | gauge | `xid` | data is present |
| `activeXidCount` | `s3_gateway_xid_metrics_active_xid_count` | counter | — | `activeXidCount > 0` |
| `xidMemoryRatio` | `s3_gateway_xid_metrics_xid_memory_ratio` | gauge | — | only when `activeXidCount > 5000` |

> The `method` label is the S3 request type (for example `get`/`put`); `error_code` is the HTTP error code.

XID data is split across several Metrics2 records ("bytes", "latency", "request count", "errors",
"xid monitoring", "percentiles") that share the same `S3GatewayXidMetrics` source. In Prometheus they all
land in the single `s3_gateway_xid_metrics_*` namespace.

---

## 3. Key parameters (class constants)

| Constant | Value | Purpose |
|---|---|---|
| `MAX_LATENCY_SAMPLES_PER_XID` | `2000` | Maximum latency samples kept per XID (FIFO, evicted via `removeFirst`) |
| `MAX_KEYS_PER_MAP` | `100000` | Key limit for each internal map; one entry is removed when exceeded |
| `CLEANUP_INTERVAL_MS` | `1 day` | How often all metrics are fully cleared |
| `ERROR_CODE_THRESHOLD` | `400` | Codes `>= 400` (including 400, 404, 500) are counted as errors |
| `XID_MONITORING_THRESHOLD` | `5000` | Threshold after which `xidMemoryRatio` is emitted and a switch to approximate sketches is recommended |
| Overflow check | every `128` records | Map sizes are checked using the bit mask `& 0x7F` |

**Behavioral details that matter for interpretation:**
- The overflow check and eviction run once every 128 records — before raising an alert about overflow, keep
  in mind that a map can briefly exceed `MAX_KEYS_PER_MAP`.
- `activeXidCount` is the size of `latencySamplesByXid` (the number of unique XIDs), not a request counter.
- `xidMemoryRatio` appears **only** when `activeXidCount > 5000`. If there are few or no active XIDs, this
  metric is not present.

---

## 4. Monitoring and alerts

### 4.1 Grafana dashboard

File: `hadoop-ozone/dist/src/main/compose/common/grafana/dashboards/Ozone-S3GatewayXIDMetrics.json`

The dashboard contains the following panels:
- **Active unique XIDs** — number of active XIDs; thresholds: green up to 5000, orange from 5000, red from 6000;
- **XID memory ratio** — ratio of active XIDs to the monitoring threshold;
- **Request count (rate)** — request throughput;
- **Total bytes (rate)** — byte throughput;
- **Request latency (ms)** — average latency (avg) and the p50/p95/p99 percentiles;
- **Error count (rate)** — error throughput.

The dashboard is picked up automatically through `provisioning/dashboards/dashboards.yml`
(the `/var/lib/grafana/dashboards` directory).

### 4.2 Prometheus rules

File: `hadoop-ozone/dist/src/main/compose/ozone/rules/s3-gateway-xid-alerts.yml`

| Alert | Expression | Duration | Severity |
|---|---|---|---|
| `S3GatewayHighActiveXidCount` | `s3_gateway_xid_metrics_active_xid_count > 5000` | 5m | warning |
| `S3GatewayHighXidMemoryRatio` | `s3_gateway_xid_metrics_xid_memory_ratio > 1` | 5m | critical |
| `S3GatewayHighErrorRate` | `sum(rate(...error_count[5m])) / clamp_min(sum(rate(...count_requests[5m])), 1) > 0.05` | 10m | warning |

Rules are enabled via the `rule_files` section in `hadoop-ozone/dist/src/main/compose/ozone/prometheus.yml`
plus mounting the `./rules` directory in `monitoring.yaml`.

> **Checking exposure:** `metrics_path: /prom`; for S3 Gateway — `s3g:9878/prom` (compose `ozone/prometheus.yml`).
> Make sure the `s3g` target actually serves metrics.

---

## 5. Operations / Runbook

### 5.1 Verify that metrics are flowing

```bash
curl -s http://<s3g-host>:9878/prom | grep s3_gateway_xid_metrics_
```

When there are XID requests, the result should be non-empty. If it is empty, see section 5.4.

### 5.2 Alert `S3GatewayHighActiveXidCount` (>5000 XIDs)

- **What it means:** S3 Gateway holds more than 5000 unique XIDs in memory (`latencySamplesByXid` is growing).
- **Risks:** increasing heap usage (up to ~160 MB at 100K XIDs).
- **Actions:**
  1. Check the `active_xid_count` trend on the dashboard.
  2. Reduce the number of unique XIDs among clients (are XIDs duplicated? Are there accidental or stray values?).
  3. If the growth is steady, start migrating to approximate sketches (HdrHistogram).

### 5.3 Alert `S3GatewayHighErrorRate` (>5% over 5 minutes)

- **What it means:** the share of failed S3 requests exceeded 5%.
- **How to find the cause:** `sum by (error_code) (rate(s3_gateway_xid_metrics_error_count[5m]))`.
- **Actions:** distinguish client-side 4xx errors (often not an incident) from 5xx errors (infrastructure or
  OM backend problems).

### 5.4 Metrics missing or empty

1. Confirm that S3 Gateway receives requests with the XID header.
2. Check `/prom` exposure on `s3g`.
3. Check that the source is registered (see `OzoneMetricsSystem`).
4. If `active_xid_count == 0`, XID monitoring metrics are intentionally absent (see section 3).

### 5.5 Clearing data

- Periodic cleanup runs automatically once a day (`CLEANUP_INTERVAL_MS`).
- A manual forced reset is possible via the test-only methods `clearMetrics()`/`setLastCleanupTime()`, but
  they are **not used in production unless necessary** (they are not publicly exposed).

---

## 6. Test coverage (summary)

Tests: `hadoop-ozone/s3gateway/src/test/java/org/apache/hadoop/ozone/s3/metrics/TestS3GatewayXidMetrics.java` —
**42 invocations / 42 green, BUILD SUCCESS**. See the separate document `s3-gateway-xid-metrics-test-plan.md`
for details.

---

## 7. Known limitations

- Eviction on overflow does not guarantee strict FIFO ordering: `ConcurrentHashMap` does not retain insertion
  order, so the evicted entry is whichever one the iterator returns first.
- `xidMemoryRatio` only appears above the monitoring threshold; below it you cannot chart the XID memory
  footprint smoothly.
- At very high `active_xid_count`, latency samples are truncated to 2000 per XID — percentiles describe the
  latest window, not the full history.