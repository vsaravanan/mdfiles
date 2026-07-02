# Java Backend Interview Q&A — Production Troubleshooting

> **Date:** June 29, 2026  
> **Focus:** Real-world production debugging scenarios for senior Java/Spring Boot backend engineers.

---

## 1. Your API works perfectly locally but becomes slow only in production. What would you check first?

### Root Causes to Investigate
- **Network latency** between services (cross-AZ / cross-region calls).
- **Database connection pool** exhaustion or slow queries under real data volume.
- **Thread pool saturation** (Tomcat / async executors).
- **GC pauses** due to larger heap or different memory profile.
- **Missing indexes** that only surface with production-scale data.
- **Cold caches** after a deployment or eviction storm.
- **Resource limits** (CPU throttling in containers, cgroup limits).

### What to Check First
1. `APM traces` (Datadog, New Relic, Jaeger) → find the slowest span.
2. `Thread dump` during slowness → look for BLOCKED/WAITING threads.
3. `DB slow query log` + connection pool metrics.
4. `GC logs` for long STW pauses.

### Code: HikariCP pool + timeout tuning

```java
@Configuration
public class DataSourceConfig {

    @Bean
    public HikariDataSource dataSource() {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl("jdbc:postgresql://prod-db:5432/orders");
        cfg.setUsername(System.getenv("DB_USER"));
        cfg.setPassword(System.getenv("DB_PASS"));

        // Production-tuned pool
        cfg.setMaximumPoolSize(20);              // rule of thumb: (cores * 2) + disk_spindle
        cfg.setMinimumIdle(5);
        cfg.setConnectionTimeout(3_000);         // fail fast instead of hanging
        cfg.setIdleTimeout(600_000);
        cfg.setMaxLifetime(1_800_000);
        cfg.setLeakDetectionThreshold(10_000);   // log connection leaks

        return new HikariDataSource(cfg);
    }
}
```

---

## 2. Kafka consumers are running normally, but message lag keeps increasing. Why?

### Common Causes
- **Processing slower than production rate** (downstream DB / HTTP call is the bottleneck).
- **Partition imbalance** — one consumer handles most partitions.
- **Frequent rebalances** due to `max.poll.interval.ms` being exceeded.
- **`max.poll.records` too high** → long processing per poll.
- **Consumer stuck in a loop** retrying the same failing record (poison pill).

### Code: Tuned consumer + graceful shutdown

```java
@Bean
public ConcurrentKafkaListenerContainerFactory<String, String> kafkaFactory(
        ConsumerFactory<String, String> cf) {
    var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
    factory.setConsumerFactory(cf);
    factory.setConcurrency(6);                  // match partition count
    factory.getContainerProperties()
           .setPollTimeout(1_000);
    factory.getContainerProperties()
           .setAckMode(ContainerProperties.AckMode.RECORD);
    return factory;
}

@KafkaListener(topics = "orders", groupId = "order-processor")
public void process(ConsumerRecord<String, String> record) {
    try {
        orderService.handle(record.value());
    } catch (TransientException e) {
        throw e; // will be retried
    } catch (Exception e) {
        deadLetterPublisher.send(record, e);     // poison pill → DLQ
    }
}
```

Consumer config hints:
```properties
max.poll.records=100
max.poll.interval.ms=300000
session.timeout.ms=30000
```

---

## 3. Database connections suddenly get exhausted during peak traffic. What could cause this?

### Likely Causes
- **Connection leak** — connections not returned (missing `close()` / no try-with-resources).
- **Slow queries** holding connections too long.
- **Missing transaction boundaries** → connection held for entire request.
- **Pool too small** for the concurrency level.
- **Long-running background jobs** sharing the same pool.

### Code: Safe connection usage + separate pools

```java
@Repository
public class OrderRepository {

    private final JdbcTemplate jdbc;

    public Order findById(Long id) {
        // try-with-resources ensures connection is always released
        return jdbc.queryForObject(
            "SELECT * FROM orders WHERE id = ?",
            new RowMapper<>() { /* ... */ },
            id
        );
    }
}

// Separate pool for batch jobs so they don't starve API traffic
@Bean("batchDataSource")
public DataSource batchDataSource() {
    return buildPool(5, "batch-pool-");
}

@Bean("apiDataSource")
@Primary
public DataSource apiDataSource() {
    return buildPool(30, "api-pool-");
}
```

Monitoring tip: expose Hikari metrics via Micrometer and alert on `hikaricp_connections_pending`.

---

## 4. Autoscaling creates more pods, but response time still keeps increasing.

### Diagnosis
Scaling out only helps if the bottleneck is **CPU / per-pod compute**. If the bottleneck is a **shared resource**, more pods just fight for it harder.

### Shared bottlenecks to look for
- Database (connection pool, locks, IOPS).
- Redis / cache (single-threaded, hitting max memory).
- Third-party API with strict rate limits.
- Message broker.
- Network egress.

### Code: HPA based on the RIGHT metric

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: orders-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: orders
  minReplicas: 3
  maxReplicas: 30
  metrics:
    - type: Pods
      pods:
        metric:
          name: http_requests_per_second   # custom metric, not CPU
        target:
          type: AverageValue
          averageValue: "500"
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300      # avoid flapping
```

Also check for **cold starts** (Spring context init, JIT warm-up, cache fill) — use readiness gates and pre-warming.

---

## 5. Retry logic starts creating duplicate payment transactions during failures.

### The Problem
Naive retry + non-idempotent downstream = **double charge**.

### Solution: Idempotency key pattern
Every request carries a client-generated unique key; the payment gateway rejects replays.

### Code: Idempotent payment call

```java
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentGateway gateway;
    private final PaymentAttemptRepository attempts;

    @Transactional
    public PaymentResult charge(PaymentCommand cmd) {
        // 1. Idempotency key = stable per business intent
        String idemKey = "order-" + cmd.getOrderId() + "-v" + cmd.getVersion();

        // 2. Check if we already succeeded
        return attempts.findByKey(idemKey)
            .map(PaymentAttempt::getResult)
            .orElseGet(() -> executeAndRecord(idemKey, cmd));
    }

    private PaymentResult executeAndRecord(String idemKey, PaymentCommand cmd) {
        PaymentResult result = gateway.charge(
            ChargeRequest.builder()
                .idempotencyKey(idemKey)         // gateway enforces uniqueness
                .amount(cmd.getAmount())
                .currency(cmd.getCurrency())
                .build()
        );

        attempts.save(new PaymentAttempt(idemKey, result));
        return result;
    }
}
```

Combine with **Resilience4j retry** configured for idempotent-safe errors only:

```java
@Retry(name = "payment", fallbackMethod = "fallback")
public PaymentResult callGateway(ChargeRequest req) { ... }
```

---

## 6. A scheduled job suddenly starts executing multiple times after scaling.

### Cause
Each pod runs its own `@Scheduled` instance → N pods = N executions.

### Solutions
1. **Distributed lock** (Redis / DB) around the job body.
2. **Leader election** (Spring Integration, ShedLock).
3. **External scheduler** (XXL-Job, Quartz clustered, Temporal).

### Code: ShedLock (recommended)

```java
@Configuration
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "10m")
public class SchedulerConfig {}

@Component
public class NightlyReconciler {

    @Scheduled(cron = "0 0 2 * * *")
    @SchedulerLock(
        name = "nightlyReconcile",
        lockAtMostFor = "15m",
        lockAtLeastFor = "30s"
    )
    public void reconcile() {
        // only one pod in the cluster runs this at a time
        reconciliationService.run();
    }
}
```

DB schema (Postgres):
```sql
CREATE TABLE shedlock (
    name       VARCHAR(64)  NOT NULL PRIMARY KEY,
    lock_until TIMESTAMP    NOT NULL,
    locked_at  TIMESTAMP    NOT NULL,
    locked_by  VARCHAR(255) NOT NULL
);
```

---

## 7. One slow downstream service starts affecting the entire platform.

### Pattern: Cascading failure
Thread pools fill up waiting on the slow service → all APIs degrade.

### Defense in depth
- **Timeouts** on every outbound call.
- **Circuit breaker** to fail fast.
- **Bulkhead** to isolate thread pools.
- **Fallback / cached response**.

### Code: Resilience4j

```java
@Service
public class RecommendationClient {

    private final RestTemplate rest;
    private final CircuitBreaker cb;

    public Recommendations get(Long userId) {
        return Supplier.ofCallable(() ->
                rest.getForObject("/recs?u=" + userId, Recommendations.class))
            .decorate(cb)
            .recover(throwable -> Recommendations.fallback(userId))
            .get();
    }
}
```

```yaml
resilience4j:
  circuitbreaker:
    instances:
      recommendation:
        slidingWindowSize: 20
        failureRateThreshold: 50
        waitDurationInOpenState: 30s
        permittedNumberOfCallsInHalfOpenState: 5
  timelimiter:
    instances:
      recommendation:
        timeoutDuration: 800ms
```

---

## 8. APIs randomly return 500 errors, but infrastructure looks healthy.

### Likely Causes
- **Unhandled exceptions** (NPE, `IllegalStateException`, data format errors).
- **Race conditions** in concurrent code.
- **Third-party API returning unexpected payload**.
- **Thread pool rejection** (`RejectedExecutionException`) — often misreported as 500.
- **Serialization failures** (Jackson on unexpected nulls).

### Code: Global exception handler + structured error

```java
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(TransientException.class)
    public ResponseEntity<ApiError> handleTransient(TransientException ex,
                                                    HttpServletRequest req) {
        log.warn("Transient error on {} {}: {}", req.getMethod(), req.getRequestURI(),
                 ex.getMessage());
        return ResponseEntity.status(503).body(ApiError.of("TRY_LATER", ex));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleUnknown(Exception ex,
                                                  HttpServletRequest req) {
        // Include correlationId from MDC so we can trace it
        String cid = MDC.get("correlationId");
        log.error("Unhandled error cid={} uri={}", cid, req.getRequestURI(), ex);
        return ResponseEntity.status(500)
            .body(ApiError.of("INTERNAL", cid));
    }
}
```

---

## 9. Health checks pass, but users still face failures.

### Why it happens
`/actuator/health` only tells you **the process is alive**. It does not guarantee:
- The DB is reachable.
- Redis is reachable.
- The service can actually serve traffic (e.g., thread pool exhausted).

### Fix: Deep readiness + liveness split

```yaml
management:
  endpoint:
    health:
      probes:
        enabled: true
      group:
        readiness:
          include: readinessState, db, redis, diskSpace
          showDetails: always
        liveness:
          include: livenessState        # lightweight — only "am I deadlocked?"
```

Kubernetes probes:
```yaml
livenessProbe:
  httpGet: { path: /actuator/health/liveness, port: 8080 }
  periodSeconds: 10
readinessProbe:
  httpGet: { path: /actuator/health/readiness, port: 8080 }
  periodSeconds: 5
startupProbe:
  httpGet: { path: /actuator/health, port: 8080 }
  failureThreshold: 30
```

Rule of thumb: **liveness = "kill me if stuck"**, **readiness = "send traffic only if I'm truly ready"**.

---

## 10. Cache improves performance initially, but later starts returning stale data.

### Causes
- **No TTL** → cached forever.
- **No invalidation** on writes.
- **Cache-aside race** (read stale, then write).
- **Multiple caches** (L1 local + L2 Redis) out of sync.

### Code: Cache-aside with TTL + event-driven invalidation

```java
@Service
@CacheConfig(cacheNames = "products")
public class ProductService {

    @Cacheable(key = "#id", unless = "#result == null")
    public Product getById(Long id) {
        return repo.findById(id).orElse(null);
    }

    @CachePut(key = "#product.id")
    @Transactional
    public Product update(Product product) {
        return repo.save(product);
    }

    @CacheEvict(key = "#id")
    public void evict(Long id) { repo.deleteById(id); }
}
```

```yaml
spring:
  cache:
    caffeine:
      spec: maximumSize=10000,expireAfterWrite=60s,expireAfterAccess=30s
    redis:
      time-to-live: 300000          # 5 min hard cap
```

For strong consistency, publish an invalidation event on writes:
```java
applicationEventPublisher.publishEvent(new ProductUpdatedEvent(product.getId()));
// listeners → cache.invalidate(id) on every node
```

---

## 11. Logs exist everywhere, but debugging is still hard.

### Why
- **No correlation ID** → impossible to stitch a request across services.
- **Unstructured logs** → grep hell.
- **Wrong log levels** (everything is INFO, or DEBUG in prod).
- **No aggregation** (logs trapped on pods).
- **Missing context** (userId, orderId not in logs).

### Code: MDC + structured JSON logging

```java
@Component
public class CorrelationFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest req,
                                    HttpServletResponse res,
                                    FilterChain chain)
            throws ServletException, IOException {
        String cid = Optional.ofNullable(req.getHeader("X-Correlation-Id"))
                             .orElseGet(UUID::toString);
        MDC.put("correlationId", cid);
        MDC.put("userId", currentUser());
        try {
            chain.doFilter(req, res);
        } finally {
            res.setHeader("X-Correlation-Id", cid);
            MDC.clear();
        }
    }
}
```

`logback-spring.xml` — JSON output:
```xml
<configuration>
  <appender name="JSON" class="ch.qos.logback.core.ConsoleAppender">
    <encoder class="net.logstash.logback.encoder.LogstashEncoder">
      <includeMdcKeyName>correlationId</includeMdcKeyName>
      <includeMdcKeyName>userId</includeMdcKeyName>
    </encoder>
  </appender>
  <root level="INFO"><appender-ref ref="JSON"/></root>
</configuration>
```

Ship to **ELK / Loki / Datadog**, and every log line becomes searchable by `correlationId`.

---

## Summary Cheat-Sheet

| # | Symptom | First thing to check |
|---|---------|----------------------|
| 1 | Slow in prod only | APM trace → slowest span |
| 2 | Kafka lag growing | Consumer throughput vs. producer rate |
| 3 | DB pool exhausted | Leak detection + slow queries |
| 4 | More pods ≠ faster | Shared bottleneck (DB/cache/3rd party) |
| 5 | Duplicate payments | Idempotency key on every call |
| 6 | Job runs N times | Distributed lock (ShedLock) |
| 7 | Slow downstream kills all | Timeout + circuit breaker + bulkhead |
| 8 | Random 500s | Global exception handler + correlation ID |
| 9 | Health OK, users fail | Deep readiness probe |
|10 | Stale cache | TTL + write-through invalidation |
|11 | Logs useless | Correlation ID + structured JSON |

---

*Generated for Java backend interview preparation — June 2026.*