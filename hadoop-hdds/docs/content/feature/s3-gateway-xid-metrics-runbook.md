# Инструкция по сопровождению S3 Gateway XID Metrics

- **Версия документа:** v1.0
- **Дата:** 2026-08-04
- **Область:** мониторинг и эксплуатация метрик класса `S3GatewayXidMetrics` (Apache Ozone)
- **Связанные файлы:** `Ozone-S3GatewayXIDMetrics.json`, `s3-gateway-xid-alerts.yml`

---

## 1. Назначение

Сервис S3 Gateway может принимать заголовок XID (идентификатор интернет-сессии внешнего клиента) и отслеживать
агрегированную статистику по каждому XID: объём переданных байт, число запросов, ошибок, латентность (включая
перцентили p50/p95/p99). Класс `S3GatewayXidMetrics` — потокобезопасный singleton, реализующий `MetricsSource`,
регистрируется в одзоне `OzoneMetricsSystem`.

Документ описывает: что измеряется, какие Prometheus-имена использовать в графах и алертах, какие пороги важны,
и какой порядок действий при инцидентах.

---

## 2. Трансляция имён Metrics2 → Prometheus

Имена метрик Hadoop Metrics2 транслируются в Prometheus по правилу
`<record_source_name_in_snake_case>_<metric_name_in_snake_case>`.

Для данного источника `SOURCE_NAME = "S3GatewayXidMetrics"` → **префикс `s3_gateway_xid_metrics_`**.

### 2.1 Таблица метрик

| Metrics2-имя | Prometheus-имя | Тип | Labels | Появляется когда |
|---|---|---|---|---|
| `sum_bytes` | `s3_gateway_xid_metrics_sum_bytes` | counter | `xid`, `method` | всегда при наличии записей |
| `count_requests` | `s3_gateway_xid_metrics_count_requests` | counter | `xid` | всегда при наличии записей |
| `error_count` | `s3_gateway_xid_metrics_error_count` | counter | `xid`, `error_code` | при `errorCode >= 400` |
| `average_latency` | `s3_gateway_xid_metrics_average_latency` | gauge | `xid` | всегда при наличии записей |
| `request_latency_ms_p50` | `s3_gateway_xid_metrics_request_latency_ms_p50` | gauge | `xid` | всегда при наличии записей |
| `request_latency_ms_p95` | `s3_gateway_xid_metrics_request_latency_ms_p95` | gauge | `xid` | всегда при наличии записей |
| `request_latency_ms_p99` | `s3_gateway_xid_metrics_request_latency_ms_p99` | gauge | `xid` | всегда при наличии записей |
| `activeXidCount` | `s3_gateway_xid_metrics_active_xid_count` | counter | — | при `activeXidCount > 0` |
| `xidMemoryRatio` | `s3_gateway_xid_metrics_xid_memory_ratio` | gauge | — | только при `activeXidCount > 5000` |

> Label `method` соответствует типу S3-запроса (например `get`/`put`), `error_code` — HTTP-коду ошибки.

Набор данных XID разбивается на **рекорды** (Record 1 «bytes» / Record 2 «latency» / Record 3 «request count» /
Record 4 «errors» / Record 5 «xid monitoring» / Record 6 «percentiles») с одним и тем же источником `S3GatewayXidMetrics`;
в Prometheus они сливаются в единое пространство имён `s3_gateway_xid_metrics_*`.

---

## 3. Ключевые параметры (константы класса)

| Константа | Значение | Назначение |
|---|---|---|
| `MAX_LATENCY_SAMPLES_PER_XID` | `2000` | Макс. число семплов латентности на один XID (FIFO, через `removeFirst`) |
| `MAX_KEYS_PER_MAP` | `100000` | Лимит ключей в каждой внутренней map; при превышении — эвикция одной записи |
| `CLEANUP_INTERVAL_MS` | `1 day` | Периодичность полной очистки метрик |
| `ERROR_CODE_THRESHOLD` | `400` | Коды `>= 400` (в т.ч. 400, 404, 500) учитываются как ошибки |
| `XID_MONITORING_THRESHOLD` | `5000` | Порог, после которого эмитится `xidMemoryRatio` и рекомендуется алерт |
| Эвикция | раз в `128` записей | Проверка переполнения карт выполняется через битовую маску `& 0x7F` |

**Поведенческие особенности, важные для интерпретации:**
- Эвикция «устаревшего» ключа происходит один раз на 128 записей — перед алертом о переполнении помните, что
  карта может кратковременно превышать `MAX_KEYS_PER_MAP`.
- `activeXidCount` — это размер `latencySamplesByXid` (уникальные XID), НЕ счётчик запросов.
- `xidMemoryRatio` появляется **только** при `activeXidCount > 5000` (если 0 — метрика отсутствует вовсе).

---

## 4. Мониторинг и алерты

### 4.1 Дашборд Grafana

Файл: `hadoop-ozone/dist/src/main/compose/common/grafana/dashboards/Ozone-S3GatewayXIDMetrics.json`

Дашборд содержит панели:
- Active unique XIDs (с порогами: зелёный → 5000 → 6000 красный);
- XID memory ratio;
- Request count (rate);
- Total bytes (rate);
- Request latency (avg + p50/p95/p99);
- Error count (rate).

Дашборд подхватывается автоматически через `provisioning/dashboards/dashboards.yml` (мнокаталог `/var/lib/grafana/dashboards`).

### 4.2 Правила Prometheus

Файл: `hadoop-ozone/dist/src/main/compose/ozone/rules/s3-gateway-xid-alerts.yml`

| Alert | Выражение | Продолжительность | Severity |
|---|---|---|---|
| `S3GatewayHighActiveXidCount` | `s3_gateway_xid_metrics_active_xid_count > 5000` | 5m | warning |
| `S3GatewayHighXidMemoryRatio` | `s3_gateway_xid_metrics_xid_memory_ratio > 1` | 5m | critical |
| `S3GatewayHighErrorRate` | `sum(rate(...error_count[5m])) / clamp_min(sum(rate(...count_requests[5m])), 1) > 0.05` | 10m | warning |

Подключение: секция `rule_files` в `hadoop-ozone/dist/src/main/compose/ozone/prometheus.yml` +
монтирование каталога `./rules` в `monitoring.yaml`.

> **Validation tip:** точки экспорта метрик — `metrics_path: /prom`, для S3 Gateway — `s3g:9878/prom`
> (compose `ozone/prometheus.yml`). Проверьте, что target «s3g» действительно отдаёт метрики.

---

## 5. Сопровождение / Runbook

### 5.1 Проверка «метрики идут»

```bash
curl -s http://<s3g-host>:9878/prom | grep s3_gateway_xid_metrics_
```

Ожидаем непустой результат при наличии XID-запросов. Если пусто — см. п. 5.4.

### 5.2 Алерт `S3GatewayHighActiveXidCount` (>5000 XID)

- **Что значит:** в памяти S3 Gateway хранится более 5000 уникальных XID (т.е. растут `latencySamplesByXid`).
- **Риски:** рост потребления кучи (до ~160MB при 100K XID).
- **Действия:**
  1. Посмотреть `active_xid_count` в дашборде на динамике.
  2. Снизить число уникальных XID у клиентов (XID продублированы? лишние сущности?).
  3. Если рост устойчивый — инициировать миграцию на approximate-sketch (HdrHistogram).

### 5.3 Алерт `S3GatewayHighErrorRate` (>5% за 5м)

- **Что значит:** доля неуспешных S3-запросов превысила 5%.
- **Найти причину:** `sum by (error_code) (rate(s3_gateway_xid_metrics_error_count[5m]))`.
- **Действия:** проверить ошибки 4xx (клиентские, часто не инцидент) vs 5xx (инфраструктура/бэкенд OM).

### 5.4 Метрики отсутствуют / пусто

1. Проверить, что S3 Gateway получает запросы с заголовком XID.
2. Проверить экспозицию `/prom` на `s3g`.
3. Проверить, что источник зарегистрирован (см. `OzoneMetricsSystem`).
4. При `active_xid_count == 0` метрики мониторинга XID отсутствуют намеренно (см. п. 3).

### 5.5 Очистка данных

- Периодическая очистка выполняется автоматически раз в сутки (`CLEANUP_INTERVAL_MS`).
- Ручной принудительный сброс выполняется через тестовый вызов `clearMetrics()`/`setLastCleanupTime()` —
  **не используется в проде без необходимости** (публично не экспонируется).

---

## 6. Тестирование качества (кратко)

Покрытие: `hadoop-ozone/s3gateway/src/test/java/org/apache/hadoop/ozone/s3/metrics/TestS3GatewayXidMetrics.java`,
**42/42 green, BUILD SUCCESS**. См. отдельный документ `s3-gateway-xid-metrics-test-plan.md`.

---

## 7. Известные ограничения

- Эвикция при переполнении не гарантирует строгий FIFO (ConcurrentHashMap не хранит порядок вставки) —
  порядок удаления соответствует обходу итератора.
- `xidMemoryRatio` появляется только выше порога; до порога нельзя построить плавный график «занятости памяти XID».
- При очень высоком `active_xid_count` семплы латентности усекаются до 2000 на XID — перцентили описывают окно,
  а не всю историю.