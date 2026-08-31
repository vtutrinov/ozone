# Инструкция по сопровождению S3 Gateway XID Metrics

- **Версия документа:** v1.1
- **Дата:** 2026-08-04
- **Область:** мониторинг и эксплуатация метрик класса `S3GatewayXidMetrics` (Apache Ozone)
- **Связанные файлы:** `Ozone-S3GatewayXIDMetrics.json`, `s3-gateway-xid-alerts.yml`

---

## 1. Назначение

S3 Gateway умеет принимать заголовок XID — идентификатор сессии внешнего клиента. Для каждого XID сервис
накапливает агрегированную статистику: объём переданных байт, число запросов, количество ошибок и задержку
ответов (включая перцентили p50/p95/p99).

Класс `S3GatewayXidMetrics` — потокобезопасный singleton, реализующий `MetricsSource`. Он регистрируется
в `OzoneMetricsSystem` в момент первого обращения через `getInstance()`.

Документ описывает: какие метрики собираются, какие Prometheus-имена использовать в графах и алертах, какие
пороговые значения считать критичными и что делать при инцидентах (runbook).

---

## 2. Трансляция имён Metrics2 → Prometheus

Имена метрик Hadoop Metrics2 транслируются в Prometheus по правилу
`<record_source_name_in_snake_case>_<metric_name_in_snake_case>`.

Для данного источника `SOURCE_NAME = "S3GatewayXidMetrics"` префикс в Prometheus —
`s3_gateway_xid_metrics_`.

### 2.1 Таблица метрик

| Metrics2-имя | Prometheus-имя | Тип | Labels | Появляется когда |
|---|---|---|---|---|
| `sum_bytes` | `s3_gateway_xid_metrics_sum_bytes` | counter | `xid`, `method` | при наличии записей |
| `count_requests` | `s3_gateway_xid_metrics_count_requests` | counter | `xid` | при наличии записей |
| `error_count` | `s3_gateway_xid_metrics_error_count` | counter | `xid`, `error_code` | при `errorCode >= 400` |
| `average_latency` | `s3_gateway_xid_metrics_average_latency` | gauge | `xid` | при наличии записей |
| `request_latency_ms_p50` | `s3_gateway_xid_metrics_request_latency_ms_p50` | gauge | `xid` | при наличии записей |
| `request_latency_ms_p95` | `s3_gateway_xid_metrics_request_latency_ms_p95` | gauge | `xid` | при наличии записей |
| `request_latency_ms_p99` | `s3_gateway_xid_metrics_request_latency_ms_p99` | gauge | `xid` | при наличии записей |
| `activeXidCount` | `s3_gateway_xid_metrics_active_xid_count` | counter | — | при `activeXidCount > 0` |
| `xidMemoryRatio` | `s3_gateway_xid_metrics_xid_memory_ratio` | gauge | — | только при `activeXidCount > 5000` |

> Label `method` — тип S3-запроса (например `get`/`put`), `error_code` — HTTP-код ошибки.

Данные XID делятся на несколько рекордов (Record «bytes», «latency», «request count», «errors»,
«xid monitoring», «percentiles») с одним и тем же источником `S3GatewayXidMetrics`. В Prometheus они
объединяются в единое пространство имён `s3_gateway_xid_metrics_*`.

---

## 3. Ключевые параметры (константы класса)

| Константа | Значение | Назначение |
|---|---|---|
| `MAX_LATENCY_SAMPLES_PER_XID` | `2000` | Максимум семплов задержки на один XID (FIFO, вытеснение через `removeFirst`) |
| `MAX_KEYS_PER_MAP` | `100000` | Лимит ключей в каждой внутренней map; при превышении удаляется одна запись |
| `CLEANUP_INTERVAL_MS` | `1 день` | Периодичность полной очистки метрик |
| `ERROR_CODE_THRESHOLD` | `400` | Коды `>= 400` (в т.ч. 400, 404, 500) учитываются как ошибки |
| `XID_MONITORING_THRESHOLD` | `5000` | Порог эмиссии `xidMemoryRatio` и рекомендации перейти на approximate-скетчи |
| Проверка переполнения | раз в `128` записей | Проверка размеров карт выполняется через битовую маску `& 0x7F` |

**Поведенческие особенности, важные для интерпретации:**
- Проверка переполнения и эвикция происходят один раз на 128 записей — перед алертом о переполнении учтите,
  что карта может кратковременно превышать `MAX_KEYS_PER_MAP`.
- `activeXidCount` — это размер `latencySamplesByXid` (число уникальных XID), а не счётчик запросов.
- `xidMemoryRatio` появляется **только** при `activeXidCount > 5000`. Если активных XID мало или нет вовсе,
  эта метрика отсутствует.

---

## 4. Мониторинг и алерты

### 4.1 Дашборд Grafana

Файл: `hadoop-ozone/dist/src/main/compose/common/grafana/dashboards/Ozone-S3GatewayXIDMetrics.json`

Дашборд содержит панели:
- **Active unique XIDs** — число активных XID; пороги: зелёный — до 5000, оранжевый — с 5000, красный — с 6000;
- **XID memory ratio** — соотношение активных XID к порогу мониторинга;
- **Request count (rate)** — скорость запросов;
- **Total bytes (rate)** — скорость передачи байт;
- **Request latency (ms)** — средняя задержка (avg) и перцентили p50/p95/p99;
- **Error count (rate)** — скорость ошибок.

Дашборд подключается автоматически через `provisioning/dashboards/dashboards.yml`
(каталог `/var/lib/grafana/dashboards`).

### 4.2 Правила Prometheus

Файл: `hadoop-ozone/dist/src/main/compose/ozone/rules/s3-gateway-xid-alerts.yml`

| Alert | Выражение | Продолжительность | Severity |
|---|---|---|---|
| `S3GatewayHighActiveXidCount` | `s3_gateway_xid_metrics_active_xid_count > 5000` | 5m | warning |
| `S3GatewayHighXidMemoryRatio` | `s3_gateway_xid_metrics_xid_memory_ratio > 1` | 5m | critical |
| `S3GatewayHighErrorRate` | `sum(rate(...error_count[5m])) / clamp_min(sum(rate(...count_requests[5m])), 1) > 0.05` | 10m | warning |

Подключение правил: секция `rule_files` в `hadoop-ozone/dist/src/main/compose/ozone/prometheus.yml` +
монтирование каталога `./rules` в `monitoring.yaml`.

> **Проверка экспозиции:** `metrics_path: /prom`, для S3 Gateway — `s3g:9878/prom`
> (compose `ozone/prometheus.yml`). Убедитесь, что target `s3g` действительно отдаёт метрики.

---

## 5. Сопровождение / Runbook

### 5.1 Проверка «метрики идут»

```bash
curl -s http://<s3g-host>:9878/prom | grep s3_gateway_xid_metrics_
```

При наличии XID-запросов результат должен быть непустым. Если пусто — см. п. 5.4.

### 5.2 Алерт `S3GatewayHighActiveXidCount` (>5000 XID)

- **Что значит:** S3 Gateway хранит в памяти более 5000 уникальных XID (растёт `latencySamplesByXid`).
- **Риски:** рост потребления кучи (до ~160 MB при 100 тыс. XID).
- **Действия:**
  1. Посмотреть динамику `active_xid_count` на дашборде.
  2. Снизить число уникальных XID у клиентов (XID дублируются? есть ли случайные/лишние значения?).
  3. Если рост устойчивый — инициировать миграцию на approximate-скетчи (HdrHistogram).

### 5.3 Алерт `S3GatewayHighErrorRate` (>5% за 5 минут)

- **Что значит:** доля неуспешных S3-запросов превысила 5%.
- **Как найти причину:** `sum by (error_code) (rate(s3_gateway_xid_metrics_error_count[5m]))`.
- **Действия:** отличить клиентские ошибки 4xx (часто не инцидент) от 5xx (проблемы инфраструктуры или бэкенда OM).

### 5.4 Метрики отсутствуют / пусто

1. Убедиться, что S3 Gateway получает запросы с заголовком XID.
2. Проверить экспозицию `/prom` на `s3g`.
3. Проверить, что источник зарегистрирован (см. `OzoneMetricsSystem`).
4. Если `active_xid_count == 0`, метрики мониторинга XID отсутствуют намеренно (см. п. 3).

### 5.5 Очистка данных

- Периодическая очистка выполняется автоматически раз в сутки (`CLEANUP_INTERVAL_MS`).
- Ручной принудительный сброс возможен через тестовые методы `clearMetrics()`/`setLastCleanupTime()`,
  но **в проде без необходимости не применяется** (публично не экспонируется).

---

## 6. Тестирование качества (кратко)

Покрытие: `hadoop-ozone/s3gateway/src/test/java/org/apache/hadoop/ozone/s3/metrics/TestS3GatewayXidMetrics.java`,
**42 инвокации / 42 green, BUILD SUCCESS**. Подробности — в отдельном документе
`s3-gateway-xid-metrics-test-plan.md`.

---

## 7. Известные ограничения

- Эвикция при переполнении не гарантирует строгий FIFO: `ConcurrentHashMap` не хранит порядок вставки,
  поэтому удаляется та запись, которую первой отдаёт итератор.
- `xidMemoryRatio` появляется только выше порога мониторинга; ниже порога построить плавный график
  «занятости памяти под XID» нельзя.
- При очень высоком `active_xid_count` семплы задержки усекаются до 2000 на XID — перцентили описывают
  последнее окно, а не всю историю.