# Тест-план S3 Gateway XID Metrics

- **Версия документа:** v1.1
- **Дата:** 2026-08-04
- **Продукт:** `S3GatewayXidMetrics` (Apache Ozone, модуль `hadoop-ozone/s3gateway`)
- **Объект тестирования:** класс `S3GatewayXidMetrics` + конфигурация мониторинга
- **Связанные документы:** `s3-gateway-xid-metrics-runbook.md`, `IMPROVEMENT_PLAN.md`, `gwmetrics.md`

---

## 1. Цель

Проверить корректность агрегации S3-статистики по XID, потокобезопасность, поведение перцентилей,
эвикцию/очистку, XID-мониторинг, корректность `equals`/`hashCode` ключей и жизненный цикл singleton,
а также работоспособность артефактов мониторинга (дашборд + правила Prometheus).

## 2. Область / вне области

**В области:** функциональность `S3GatewayXidMetrics`, unit-тесты, интеграционные сценарии мониторинга.

**Вне области:** JVM-метрики S3 Gateway, RPC/SCM/OM-метрики, прочие S3-метрики (`S3GatewayMetrics`).

## 3. Окружение

- Maven (offline используем `-o`), сборка модуля: `mvn -o -pl hadoop-ozone/s3gateway -am test-compile`.
- Запуск тестов: `mvn -o -pl hadoop-ozone/s3gateway test -Dtest='TestS3GatewayXidMetrics'`.
- JUnit 5 (Jupiter), Mockito, `@ParameterizedTest`, `@NullAndEmptySource`.

> Примечание: изолированная сборка `-pl hadoop-ozone/s3gateway` (без `-am`) падает на main-коде из-за
> отсутствующих символов — штатная сборка идёт с `-am` (reactor-зависимости).

## 4. Покрытие (unit-тесты)

Носитель: `hadoop-ozone/s3gateway/src/test/java/org/apache/hadoop/ozone/s3/metrics/TestS3GatewayXidMetrics.java`.
Всего **39 методов / 42 инвокации** (2 параметризованных).

### 4.1 Агрегация запросов

| ID | Тест | Ожидаемый результат |
|---|---|---|
| A1 | `testBytesGroupedByXidAndRequestType` | Сумма байт группируется по `(xid, method)` |
| A2 | `testAverageLatencyGroupedByXid` | Средняя латентность `total/count` на XID |
| A3 | `testRequestCountGroupedByXid` | Число запросов на XID |
| A4 | `testErrorsGroupedByXidAndErrorCode` | Ошибки группируются по `(xid, error_code)` |

### 4.2 Граничные случаи агрегации

| ID | Тест | Ожидаемый результат |
|---|---|---|
| B1 | `testSuccessStatusCodesAreNotErrors` (200/201/204) | Успешные коды не пишутся в `error_count` |
| B2 | `testErrorCodeBelow400` | `errorCode < 400` не ошибка |
| B3 | `testErrorCodeAt400` | `errorCode == 400` — ошибка (граница `ERROR_CODE_THRESHOLD`) |
| B4 | `testXidNormalisedToDefault` (null/empty) | null/пустой XID → `"none"` |
| B5 | `testNullRequestType` | `null` requestType не вызывает NPE |

### 4.3 Корректность перцентилей

| ID | Тест | Ожидаемый результат |
|---|---|---|
| C1 | `testPercentileSingleSample` | Один семпл ⇒ p50=p95=p99=значение |
| C2 | `testPercentileEmptySamples` | Пусто ⇒ 0.0, без эмиссии рекордов |
| C3 | `testPercentileLinearInterpolation` | `[1..5]` ⇒ p50=3.0, p95=4.8, p99=4.96 |
| C4 | `testPercentileBoundaries` | Двухточечный mid-point (p50/p95) |
| C5 | `testPercentileCaching` | Повторные чтения переиспользуют кэш |
| C6 | `testPercentileCacheInvalidationOnOverflow` | Ротация `removeFirst` инвалидирует кэш |
| C7 | `testPercentileCacheInvalidationOnClear` | `clearMetrics` сбрасывает кэш |

### 4.4 Эвикция и очистка

| ID | Тест | Ожидаемый результат |
|---|---|---|
| D1 | `testEvictionRemovesOldestKey` | При превышении `MAX_KEYS_PER_MAP` удаляется запись |
| D2 | `testMetricsAreDeletedAfterCleanupInterval` | По истечении интервала метрики очищаются (явная проверка, что внутренние карты `bytesTotal`/`errorsTotal`/`bytesMetricKeyPool` пусты, + коллектор не эмитит рекордов) |
| D3 | `testMetricsAreNotDeletedBeforeCleanupInterval` | До интервала метрики сохраняются (явный guard, что карты НЕ очищены до наступления интервала) |
| D4 | `testClearMetricsCachesPercentiles` | `clearMetrics` очищает карты и кэш перцентил |

### 4.5 XID-мониторинг

| ID | Тест | Ожидаемый результат |
|---|---|---|
| E1 | `testXidMonitoringActiveCount` | Эмитится `activeXidCount` при `>0` XID |
| E2 | `testXidMonitoringClearResetsCount` | После очистки `activeXidCount` сбрасывается |
| E3 | `testXidMonitoringThresholdAlert` | При `activeXidCount > 5000` появляется `xidMemoryRatio` |

### 4.6 Ключи (`equals`/`hashCode`)

> Вложенные ключи `BytesMetricKey`/`RequestMetricKey` переведены в package-private `static final` —
> тесты обращаются к ним напрямую (без рефлексии). Проверяются рефлексивность, симметрия, транзитивность,
> консистентность, null-safe случаи (`bothDiff`), согласованный `hashCode` и spread по всем полям.

| ID | Тест | Ожидаемый результат |
|---|---|---|
| F1 | `testBytesMetricKeyEquals` | Полный контракт `equals` для `BytesMetricKey` |
| F2 | `testBytesMetricKeyHashCode` | Согласованный `hashCode` + spread по полям |
| F3 | `testRequestMetricKeyEquals` | Полный контракт `equals` для `RequestMetricKey` |
| F4 | `testRequestMetricKeyHashCode` | Согласованный `hashCode` + spread по полям |

### 4.7 Singleton и жизненный цикл

| ID | Тест | Ожидаемый результат |
|---|---|---|
| G1 | `testGetInstanceReturnsSameInstance` | `getInstance()` возвращает тот же объект |
| G2 | `testDCLSingletonThreadSafety` | DCL-синглтон потокобезопасен (одна инстанция) |
| G3 | `testUnregister` | `unRegister()` идемпотентен, без исключений |

### 4.8 Конкурентность

| ID | Тест | Ожидаемый результат |
|---|---|---|
| H1 | `testGetMetricsAndClearMetricsConcurrentAccess` | Параллельные чтения+очистка без исключений |
| H2 | `testConcurrentRecordRequest` | N×M записей под одним XID — без потерь |
| H3 | `testConcurrentRecordRequestDifferentXids` | Уникальные XID без исключений |
| H4 | `testConcurrentGetMetricsAndRecordRequest` | Чтение+запись без исключений |
| H5 | `testConcurrentGetMetricsWhileRecording` | Безопасное чтение во время записи |
| H6 | `testConcurrentRecordRequestNoDataLoss` | Нет потери данных при конкуренции |

### 4.9 GC-интёрн ключей

| ID | Тест | Ожидаемый результат |
|---|---|---|
| I1 | `testBytesMetricKeyInternPooling` | Одинаковые `(xid, method)` → один экземпляр |
| I2 | `testBytesMetricKeyInternPoolClearOnClearMetrics` | Pool очищается вместе с метриками |
| I3 | `testBytesMetricKeyInternNoNPE` | `internBytesKey` устойчив к `null` |

## 5. Тестовые данные

- XID: `"xid-1"`, `"xid-2"`, ... для агрегации; `null`/`""` для нормализации.
- `errorCode`: `200, 201, 204, 399, 400, 404, 500`.
- `latencyMs`: наборы для проверки интерполяции `[1..5]`, `[10,20]`, одиночный семпл.
- Нагрузочные кейсы: выборки > `MAX_KEYS_PER_MAP` (эвикция), > 5000 XID (порог мониторинга).

## 6. Критерии приёмки

- **42/42 green** модульного класса `TestS3GatewayXidMetrics` (BUILD SUCCESS).
- Никаких `assertTrue(true, ...)`.
- **Отсутствие рефлексии** в тестах ключей: вложенные `BytesMetricKey`/`RequestMetricKey` package-private,
  вызовы прямые (в т.ч. без `setAccessible(true)`).
- Алерты/дашборд валидны (см. п. 7.2).
- Нет гонок в concurrency-тестах при многократном прогоне (`-Dtest=...`×N).

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

- Эвикция не даёт строгий FIFO (ConcurrentHashMap — произвольный порядок обхода итератора).
- `xidMemoryRatio` эмитится только при `activeXidCount > 5000`.
- Перцентили усекаются до последних 2000 семплов на XID.
- Concurrency-тесты опираются на детерминизм mock-сборщика; при flaky-прогонах рекомендован повторный запуск.

## 9. Результат

Текущее состояние: **42 инвокации, 0 failures, 0 errors, 0 skipped, BUILD SUCCESS.**