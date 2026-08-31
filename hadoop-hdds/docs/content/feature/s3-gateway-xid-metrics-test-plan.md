# Тест-план S3 Gateway XID Metrics

- **Версия документа:** v1.2
- **Дата:** 2026-08-04
- **Продукт:** `S3GatewayXidMetrics` (Apache Ozone, модуль `hadoop-ozone/s3gateway`)
- **Объект тестирования:** класс `S3GatewayXidMetrics` и конфигурация мониторинга
- **Связанные документы:** `s3-gateway-xid-metrics-runbook.md`, `IMPROVEMENT_PLAN.md`, `gwmetrics.md`

---

## 1. Цель

Проверить корректность агрегации S3-статистики по XID, потокобезопасность, расчёт перцентилей, поведение
эвикции и очистки, XID-мониторинг, корректность `equals`/`hashCode` у ключей, жизненный цикл singleton,
а также работоспособность артефактов мониторинга (дашборд и правила Prometheus).

## 2. Область и границы (in scope / out of scope)

**В области:** функциональность `S3GatewayXidMetrics`, unit-тесты, интеграционные сценарии мониторинга.

**Вне области:** JVM-метрики S3 Gateway, RPC/SCM/OM-метрики, прочие S3-метрики (`S3GatewayMetrics`).

## 3. Окружение

- Maven (offline — флаг `-o`), компиляция модуля: `mvn -o -pl hadoop-ozone/s3gateway -am test-compile`.
- Запуск тестов: `mvn -o -pl hadoop-ozone/s3gateway test -Dtest='TestS3GatewayXidMetrics'`.
- JUnit 5 (Jupiter), Mockito, `@ParameterizedTest`, `@NullAndEmptySource`.

> Примечание: изолированная сборка `-pl hadoop-ozone/s3gateway` (без `-am`) падает на main-коде из-за
> отсутствующих символов — штатная сборка выполняется с `-am` (reactor-зависимости).

## 4. Покрытие (unit-тесты)

Расположение тестов: `hadoop-ozone/s3gateway/src/test/java/org/apache/hadoop/ozone/s3/metrics/TestS3GatewayXidMetrics.java`.
Всего **39 методов / 42 инвокации** (37 тестов `@Test` + 2 параметризованных: один с набором кодов `200/201/204`
даёт 3 инвокации, второй с `@NullAndEmptySource` — 2 инвокации).

### 4.1 Агрегация запросов

| ID | Тест | Ожидаемый результат |
|---|---|---|
| A1 | `testBytesGroupedByXidAndRequestType` | Сумма байт группируется по `(xid, method)` |
| A2 | `testAverageLatencyGroupedByXid` | Средняя задержка `total/count` на XID |
| A3 | `testRequestCountGroupedByXid` | Число запросов на XID |
| A4 | `testErrorsGroupedByXidAndErrorCode` | Ошибки группируются по `(xid, error_code)` |

### 4.2 Граничные случаи агрегации

| ID | Тест | Ожидаемый результат |
|---|---|---|
| B1 | `testSuccessStatusCodesAreNotErrors` (200/201/204) | Успешные коды не попадают в `error_count` |
| B2 | `testErrorCodeBelow400` | `errorCode < 400` не считается ошибкой |
| B3 | `testErrorCodeAt400` | `errorCode == 400` — ошибка (граница `ERROR_CODE_THRESHOLD`) |
| B4 | `testXidNormalisedToDefault` (null/empty) | null/пустой XID нормализуется в `"none"` |
| B5 | `testNullRequestType` | `null` в `requestType` не вызывает NPE |

### 4.3 Корректность перцентилей

| ID | Тест | Ожидаемый результат |
|---|---|---|
| C1 | `testPercentileSingleSample` | Один семпл ⇒ p50=p95=p99=значение |
| C2 | `testPercentileEmptySamples` | Пустая выборка ⇒ 0.0, рекорды не эмитятся |
| C3 | `testPercentileLinearInterpolation` | `[1..5]` ⇒ p50=3.0, p95=4.8, p99=4.96 |
| C4 | `testPercentileBoundaries` | Двухточечный mid-point (p50/p95) |
| C5 | `testPercentileCaching` | Повторные чтения переиспользуют кэш |
| C6 | `testPercentileCacheInvalidationOnOverflow` | Ротация `removeFirst` инвалидирует кэш |
| C7 | `testPercentileCacheInvalidationOnClear` | `clearMetrics` сбрасывает кэш |

### 4.4 Эвикция и очистка

| ID | Тест | Ожидаемый результат |
|---|---|---|
| D1 | `testEvictionRemovesOldestKey` | При превышении `MAX_KEYS_PER_MAP` запись удаляется |
| D2 | `testMetricsAreDeletedAfterCleanupInterval` | После наступления интервала метрики очищаются (явная проверка, что внутренние карты `bytesTotal`/`errorsTotal`/`bytesMetricKeyPool` пусты и коллектор не эмитит рекорды) |
| D3 | `testMetricsAreNotDeletedBeforeCleanupInterval` | До наступления интервала метрики сохраняются (явный guard, что карты НЕ очищены заранее) |
| D4 | `testClearMetricsCachesPercentiles` | `clearMetrics` очищает карты и кэш перцентилей |

### 4.5 XID-мониторинг

| ID | Тест | Ожидаемый результат |
|---|---|---|
| E1 | `testXidMonitoringActiveCount` | `activeXidCount` эмитится при наличии активных XID |
| E2 | `testXidMonitoringClearResetsCount` | После очистки `activeXidCount` сбрасывается |
| E3 | `testXidMonitoringThresholdAlert` | При `activeXidCount > 5000` появляется `xidMemoryRatio` |

### 4.6 Ключи (`equals`/`hashCode`)

> Вложенные ключи `BytesMetricKey`/`RequestMetricKey` объявлены package-private `static final` — тесты
> обращаются к ним напрямую, без рефлексии. Проверяются рефлексивность, симметричность, транзитивность,
> консистентность, null-safe случаи (`bothDiff`), согласованность `hashCode` и распределение по всем полям.

| ID | Тест | Ожидаемый результат |
|---|---|---|
| F1 | `testBytesMetricKeyEquals` | Полный контракт `equals` для `BytesMetricKey` |
| F2 | `testBytesMetricKeyHashCode` | Согласованный `hashCode` + распределение по полям |
| F3 | `testRequestMetricKeyEquals` | Полный контракт `equals` для `RequestMetricKey` |
| F4 | `testRequestMetricKeyHashCode` | Согласованный `hashCode` + распределение по полям |

### 4.7 Singleton и жизненный цикл

| ID | Тест | Ожидаемый результат |
|---|---|---|
| G1 | `testGetInstanceReturnsSameInstance` | `getInstance()` возвращает один и тот же объект |
| G2 | `testDCLSingletonThreadSafety` | DCL-синглтон потокобезопасен (единственная инстанция) |
| G3 | `testUnregister` | `unRegister()` идемпотентен, без исключений |

### 4.8 Конкурентность

| ID | Тест | Ожидаемый результат |
|---|---|---|
| H1 | `testGetMetricsAndClearMetricsConcurrentAccess` | Параллельное чтение и очистка без исключений |
| H2 | `testConcurrentRecordRequest` | N×M записей под одним XID без потерь |
| H3 | `testConcurrentRecordRequestDifferentXids` | Уникальные XID без исключений |
| H4 | `testConcurrentGetMetricsAndRecordRequest` | Чтение и запись без исключений |
| H5 | `testConcurrentGetMetricsWhileRecording` | Безопасное чтение во время записи |
| H6 | `testConcurrentRecordRequestNoDataLoss` | Без потери данных при конкуренции |

### 4.9 Интернирование ключей (GC-нагрузка)

| ID | Тест | Ожидаемый результат |
|---|---|---|
| I1 | `testBytesMetricKeyInternPooling` | Одинаковые `(xid, method)` дают один экземпляр |
| I2 | `testBytesMetricKeyInternPoolClearOnClearMetrics` | Pool очищается вместе с метриками |
| I3 | `testBytesMetricKeyInternNoNPE` | `internBytesKey` устойчив к `null` |

## 5. Тестовые данные

- XID: `"xid-1"`, `"xid-2"`, ... для агрегации; `null`/`""` для проверки нормализации.
- `errorCode`: `200, 201, 204, 399, 400, 404, 500`.
- `latencyMs`: наборы для проверки интерполяции `[1..5]`, `[10,20]`, одиночный семпл.
- Нагрузочные кейсы: выборки больше `MAX_KEYS_PER_MAP` (эвикция), больше 5000 XID (порог мониторинга).

## 6. Критерии приёмки

- **42 инвокации / 42 green** модульного класса `TestS3GatewayXidMetrics` (BUILD SUCCESS).
- Отсутствуют `assertTrue(true, ...)`.
- **Нет рефлексии** в тестах ключей: вложенные `BytesMetricKey`/`RequestMetricKey` package-private,
  вызовы прямые (в т.ч. без `setAccessible(true)`).
- Алерты и дашборд валидны (см. п. 7.2).
- Нет гонок в concurrency-тестах при многократном прогоне (`-Dtest=...` ×N).

## 7. Сценарии ручной верификации

### 7.1 Локальный прогон

```bash
mvn -o -pl hadoop-ozone/s3gateway -am test-compile
mvn -o -pl hadoop-ozone/s3gateway test -Dtest='TestS3GatewayXidMetrics'
```

### 7.2 Мониторинг

1. Убедиться, что дашборд `Ozone-S3GatewayXIDMetrics.json` отображается в Grafana (provisioning).
2. Убедиться, что Prometheus «видит» правила (конфиг `rule_files`), а `s3g` отдаёт `/prom`.
3. Проверить expr-выражения по префиксу `s3_gateway_xid_metrics_` (см. runbook).

## 8. Риски и ограничения

- Эвикция не даёт строгий FIFO (`ConcurrentHashMap` — произвольный порядок обхода итератора).
- `xidMemoryRatio` эмитится только при `activeXidCount > 5000`.
- Перцентили ограничиваются последними 2000 семплов на XID.
- Concurrency-тесты опираются на детерминизм mock-сборщика; при flaky-прогонах рекомендуется перезапустить тесты.

## 9. Результат

Текущее состояние: **42 инвокации, 0 failures, 0 errors, 0 skipped, BUILD SUCCESS.**