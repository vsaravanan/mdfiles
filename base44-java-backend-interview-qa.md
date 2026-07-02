# Java Backend Interview Q&A

> Advanced distributed systems debugging questions for senior backend engineers.

---

## 1. API slow only in production

**Question:** Your API works perfectly locally but becomes slow only in production. What would you check first?

### Explanation

Production slowness that doesn't reproduce locally almost always comes from one of five root causes: **database query performance at scale**, **network latency between services**, **resource contention** (connection pools, thread pools, locks), **GC pressure under load**, or **missing/expired caches**. Start with distributed traces (Zipkin/Jaeger) to find which span is slow, then drill down. Check slow-query logs, connection pool saturation, and heap metrics.

### Key Points

- Enable distributed tracing to pinpoint the slow span
- Check DB slow-query logs and N+1 query patterns
- Inspect HikariCP pool metrics (active, idle, pending)
- Profile GC logs — frequent Full GC pauses add latency
- Compare network RTT between services in prod vs local

### Code Example

```java
// 1. Instrument with Micrometer + HikariCP metrics
@Configuration
public class DataSourceConfig {

    @Bean
    public DataSource dataSource(MeterRegistry registry) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://prod-db:5432/app");
        config.setMaximumPoolSize(20);
        config.setMinimumIdle(5);
        config.setConnectionTimeout(3000);   // 3 s — fail fast
        config.setLeakDetectionThreshold(5000); // log leaks > 5 s

        HikariDataSource ds = new HikariDataSource(config);
        // Expose pool metrics to Prometheus / Grafana
        new HikariCPMetrics(ds, "hikari", Tags.empty()).bindTo(registry);
        return ds;
    }
}

// 2. Detect N+1 queries with Hibernate statistics
@Configuration
public class JpaConfig {
    @Bean
    public Properties hibernateProperties() {
        Properties p = new Properties();
        p.put("hibernate.generate_statistics", "true");  // enable stats
        p.put("hibernate.session.events.log.LOG_QUERIES_SLOWER_THAN_MS", "100");
        return p;
    }
}

// 3. Add @Timed to every REST controller method
@RestController
@RequestMapping("/orders")
public class OrderController {

    @GetMapping("/{id}")
    @Timed(value = "order.fetch", description = "Time to fetch an order")
    public ResponseEntity<Order> getOrder(@PathVariable Long id) {
        return ResponseEntity.ok(orderService.findById(id));
    }
}

// 4. Log slow DB queries via AOP
@Aspect
@Component
public class SlowQueryAspect {

    private static final Logger log = LoggerFactory.getLogger(SlowQueryAspect.class);
    private static final long THRESHOLD_MS = 200;

    @Around("execution(* com.example.repository.*.*(..))")
    public Object profile(ProceedingJoinPoint pjp) throws Throwable {
        long start = System.currentTimeMillis();
        Object result = pjp.proceed();
        long elapsed = System.currentTimeMillis() - start;
        if (elapsed > THRESHOLD_MS) {
            log.warn("SLOW QUERY: {}.{}() took {} ms",
                pjp.getTarget().getClass().getSimpleName(),
                pjp.getSignature().getName(), elapsed);
        }
        return result;
    }
}
```

---

## 2. Kafka Consumer Lag Increasing

**Question:** Kafka consumers are running normally, but message lag keeps increasing. Why can this happen?

### Explanation

Consumer lag grows when the **production rate exceeds the consumption rate**. The consumers appear healthy (no crashes, no exceptions), yet they fall behind. Common causes: consumers spending too much time in processing logic (DB writes, HTTP calls), too few partitions to parallelize, a single slow message blocking the batch, or commit strategy causing reprocessing. Check consumer group lag via `kafka-consumer-groups.sh` or Grafana, then tune concurrency and processing logic.

### Key Points

- Production rate > consumption rate — add partitions or consumers
- One slow record blocks the whole batch — use async processing
- Synchronous external calls (DB, HTTP) inside the listener slow throughput
- Auto-commit too frequent causes reprocessing overhead
- GC pauses cause session timeouts → rebalance → lag spike

### Code Example

```java
// 1. Concurrent Kafka listener with manual ack
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> factory(
            ConsumerFactory<String, String> cf) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(cf);
        factory.setConcurrency(6);          // match partition count
        factory.getContainerProperties()
               .setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties()
               .setPollTimeout(3000);
        return factory;
    }
}

// 2. Listener with async DB write to avoid blocking the poll loop
@Component
public class OrderEventListener {

    private final OrderRepository repo;
    private final ExecutorService dbPool =
        Executors.newFixedThreadPool(10); // separate thread pool

    @KafkaListener(topics = "orders", groupId = "order-service",
                   containerFactory = "factory")
    public void consume(ConsumerRecord<String, String> record,
                        Acknowledgment ack) {
        CompletableFuture
            .runAsync(() -> repo.save(parse(record.value())), dbPool)
            .thenRun(ack::acknowledge)   // ack only after successful save
            .exceptionally(ex -> {
                log.error("Processing failed for offset {}: {}",
                          record.offset(), ex.getMessage());
                // Do NOT ack — let it retry or go to DLQ
                return null;
            });
    }
}

// 3. Monitor lag programmatically
@Scheduled(fixedDelay = 30_000)
public void reportLag() {
    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
    Map<TopicPartition, OffsetAndMetadata> committed =
        consumer.committed(new HashSet<>(partitions));

    endOffsets.forEach((tp, end) -> {
        long committed_offset = committed.getOrDefault(tp,
            new OffsetAndMetadata(0)).offset();
        long lag = end - committed_offset;
        meterRegistry.gauge("kafka.consumer.lag",
            Tags.of("partition", String.valueOf(tp.partition())), lag);
        if (lag > 10_000) {
            log.warn("HIGH LAG on partition {}: {} messages", tp.partition(), lag);
        }
    });
}
```

---

## 3. Database Connections Exhausted

**Question:** Database connections suddenly get exhausted during peak traffic. What could cause this?

### Explanation

Connection exhaustion during peak traffic is a classic resource contention problem. The connection pool has a fixed upper limit; if threads hold connections longer than necessary (long transactions, missing `close()` calls, slow downstream blocking the thread) or peak concurrency simply exceeds pool size, new requests queue up and eventually time out. Key fixes: right-size the pool, enforce short transactions, detect leaks, and consider PgBouncer/ProxySQL for connection multiplexing.

### Key Points

- Long-running transactions hold connections unnecessarily
- Connection leaks — try-with-resources not used
- Pool size too small for peak concurrency
- Slow downstream (HTTP call) inside a DB transaction
- Missing connection pool metrics — you're flying blind

### Code Example

```java
// 1. Always use try-with-resources — never leak connections
public List<Order> findPendingOrders() {
    // BAD: connection may never be returned if exception thrown
    // Connection con = dataSource.getConnection();

    // GOOD: auto-closed even on exception
    try (Connection con = dataSource.getConnection();
         PreparedStatement ps = con.prepareStatement(
             "SELECT * FROM orders WHERE status = ?")) {
        ps.setString(1, "PENDING");
        try (ResultSet rs = ps.executeQuery()) {
            List<Order> result = new ArrayList<>();
            while (rs.next()) result.add(map(rs));
            return result;
        }
    } catch (SQLException e) {
        throw new DataAccessException("Failed to fetch orders", e);
    }
}

// 2. Keep transactions short — never call HTTP inside @Transactional
@Service
public class PaymentService {

    // BAD: HTTP call inside transaction holds DB connection
    @Transactional
    public void processBad(Long orderId) {
        Order o = repo.findById(orderId).orElseThrow();
        paymentGateway.charge(o);   // HTTP call — can take seconds!
        o.setStatus("PAID");
        repo.save(o);
    }

    // GOOD: HTTP call outside transaction
    public void processGood(Long orderId) {
        Order o = transactionTemplate.execute(tx ->
            repo.findById(orderId).orElseThrow());

        PaymentResult result = paymentGateway.charge(o); // outside TX

        transactionTemplate.execute(tx -> {
            o.setStatus(result.success() ? "PAID" : "FAILED");
            return repo.save(o);
        });
    }
}

// 3. HikariCP pool sizing formula & leak detection
@Bean
public HikariDataSource hikariDataSource() {
    HikariConfig cfg = new HikariConfig();
    // Formula: connections = (core_count * 2) + effective_spindle_count
    // For 4-core machine with SSD: ~10 connections is often optimal
    cfg.setMaximumPoolSize(10);
    cfg.setMinimumIdle(2);
    cfg.setIdleTimeout(600_000);          // 10 min
    cfg.setMaxLifetime(1_800_000);        // 30 min
    cfg.setLeakDetectionThreshold(4000);  // warn if held > 4 s
    return new HikariDataSource(cfg);
}
```

---

## 4. Autoscaling Doesn't Help Response Time

**Question:** Autoscaling creates more pods, but response time still keeps increasing.

### Explanation

More pods help only if the bottleneck is **CPU or memory**. If the real bottleneck is a shared downstream resource — a single database, a slow external API, a Redis instance — adding pods just increases contention on that shared resource, making things worse. Other culprits: the HPA metric is wrong (scaling on CPU when the bottleneck is I/O), slow pod startup causes a warmup gap, or traffic isn't distributed evenly (sticky sessions, unbalanced partitions).

### Key Points

- Bottleneck is the shared DB — more pods = more contention
- Scaling on CPU but the real bottleneck is I/O or network
- Pod startup time too long — new pods miss the traffic spike
- JVM warm-up: new JVM instances start cold (no JIT, no cache)
- Stateful sessions not distributed — some pods overloaded

### Code Example

```java
// 1. Scale on the RIGHT metric — custom HPA with request latency
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: order-service-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: order-service
  minReplicas: 2
  maxReplicas: 20
  metrics:
  - type: External
    external:
      metric:
        name: http_request_duration_p99_seconds  # latency-based scaling
      target:
        type: AverageValue
        averageValue: "0.5"   # scale up if p99 > 500 ms

---
// 2. Readiness probe — only route traffic when the JVM is warm
@Component
public class WarmupReadinessIndicator implements HealthIndicator {

    private final AtomicBoolean ready = new AtomicBoolean(false);

    @PostConstruct
    public void warmUp() {
        CompletableFuture.runAsync(() -> {
            // Warm up connection pool, caches, JIT
            IntStream.range(0, 50).forEach(i -> {
                orderRepository.findTop10ByOrderByCreatedAtDesc();
            });
            ready.set(true);
            log.info("Pod warmed up — accepting traffic");
        });
    }

    @Override
    public Health health() {
        return ready.get() ? Health.up().build()
                           : Health.down().withDetail("reason", "warming up").build();
    }
}

// 3. Read replicas to offload the primary DB
@Configuration
public class RoutingDataSourceConfig {

    @Bean
    public DataSource routingDataSource(
            @Qualifier("primary") DataSource primary,
            @Qualifier("replica") DataSource replica) {

        Map<Object, Object> targets = Map.of(
            "WRITE", primary,
            "READ",  replica
        );
        AbstractRoutingDataSource ds = new AbstractRoutingDataSource() {
            @Override
            protected Object determineCurrentLookupKey() {
                // Route @Transactional(readOnly=true) to replica
                return TransactionSynchronizationManager.isCurrentTransactionReadOnly()
                    ? "READ" : "WRITE";
            }
        };
        ds.setTargetDataSources(targets);
        ds.setDefaultTargetDataSource(primary);
        return ds;
    }
}
```

---

## 5. Duplicate Payment Transactions

**Question:** Retry logic starts creating duplicate payment transactions during failures.

### Explanation

Retries are essential for resilience but dangerous for non-idempotent operations like payments. When a payment request times out, you don't know if the charge succeeded server-side. Retrying without an **idempotency key** creates duplicate charges. The solution: generate a unique key per payment attempt, send it with every request, and let the server deduplicate. Combined with a distributed lock, this guarantees exactly-once payment processing.

### Key Points

- Network timeout ≠ failure — the charge may have gone through
- Idempotency key sent with every retry prevents duplicates
- Distributed lock (Redis) prevents concurrent duplicate attempts
- Store payment intent status in DB before calling gateway
- Use outbox pattern for guaranteed exactly-once delivery

### Code Example

```java
// 1. Generate and persist idempotency key before calling gateway
@Service
@Transactional
public class PaymentService {

    private final PaymentRepository paymentRepo;
    private final PaymentGateway gateway;
    private final RedisTemplate<String, String> redis;

    public PaymentResult charge(PaymentRequest req) {
        String idempotencyKey = "pay:" + req.getOrderId() + ":" + req.getUserId();

        // Distributed lock — only one thread processes this payment
        Boolean acquired = redis.opsForValue()
            .setIfAbsent(idempotencyKey + ":lock", "1", Duration.ofSeconds(30));

        if (!Boolean.TRUE.equals(acquired)) {
            throw new ConcurrentPaymentException("Payment already in progress");
        }

        try {
            // Check if already processed
            Optional<Payment> existing = paymentRepo.findByIdempotencyKey(idempotencyKey);
            if (existing.isPresent()) {
                log.info("Idempotent hit — returning cached result for key {}",
                         idempotencyKey);
                return PaymentResult.from(existing.get());
            }

            // Create a PENDING record BEFORE calling gateway
            Payment payment = paymentRepo.save(Payment.builder()
                .idempotencyKey(idempotencyKey)
                .amount(req.getAmount())
                .status(PaymentStatus.PENDING)
                .build());

            // Call gateway with idempotency key in header
            GatewayResponse resp = gateway.charge(req, idempotencyKey);

            // Update status based on response
            payment.setStatus(resp.isSuccess()
                ? PaymentStatus.COMPLETED : PaymentStatus.FAILED);
            payment.setGatewayTxId(resp.getTransactionId());
            paymentRepo.save(payment);

            return PaymentResult.from(payment);
        } finally {
            redis.delete(idempotencyKey + ":lock");
        }
    }
}

// 2. Resilience4j retry — only retry on network errors, not on business failures
@Bean
public RetryConfig paymentRetryConfig() {
    return RetryConfig.custom()
        .maxAttempts(3)
        .waitDuration(Duration.ofMillis(500))
        .exponentialBackoff(2.0, Duration.ofSeconds(5))
        .retryOnException(e -> e instanceof NetworkException)  // safe to retry
        .ignoreExceptions(PaymentDeclinedException.class,      // not retryable
                          InvalidCardException.class)
        .build();
}
```

---

## 6. Scheduled Job Runs Multiple Times

**Question:** A scheduled job suddenly starts executing multiple times after scaling.

### Explanation

Spring's `@Scheduled` runs on **every pod** by default. Scale from 1 to 5 pods and the job fires 5 times simultaneously. Solutions: use **ShedLock** (DB-backed distributed lock) or **Quartz** (clustered scheduler) so only one node acquires the lock and executes the job. The lock must be released even on failure, and the lock duration must exceed the maximum expected job runtime.

### Key Points

- @Scheduled has no cluster awareness — every pod runs it
- ShedLock: one row per job in the DB, acquired via optimistic lock
- Lock duration must be ≥ max job runtime to prevent overlap
- Always release lock in a finally block
- Quartz clustering uses a shared DB for leader election

### Code Example

```java
// 1. Add ShedLock dependency (Maven):
// <dependency>
//   <groupId>net.javacrumbs.shedlock</groupId>
//   <artifactId>shedlock-spring</artifactId>
// </dependency>

// 2. Configure ShedLock with JDBC store
@Configuration
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "PT10M") // 10 min max
public class SchedulerConfig {

    @Bean
    public LockProvider lockProvider(DataSource dataSource) {
        // Creates shedlock table automatically if using JdbcTemplateLockProvider
        return new JdbcTemplateLockProvider(
            JdbcTemplateLockProvider.Configuration.builder()
                .withJdbcTemplate(new JdbcTemplate(dataSource))
                .usingDbTime()   // use DB clock, not local clock
                .build()
        );
    }
}

// 3. Annotate the job — only ONE pod executes it cluster-wide
@Component
public class ReportGenerationJob {

    @Scheduled(cron = "0 0 2 * * *")  // every night at 2 AM
    @SchedulerLock(
        name = "reportGenerationJob",
        lockAtLeastFor = "PT5M",   // hold lock at least 5 min (prevent rerun)
        lockAtMostFor  = "PT15M"   // release after 15 min even if pod dies
    )
    public void generateDailyReport() {
        log.info("Running report generation on pod: {}",
                 System.getenv("HOSTNAME"));
        reportService.generate();
    }
}

// 4. Verify lock acquisition in tests
@SpringBootTest
class ReportJobTest {

    @Autowired private ReportGenerationJob job;
    @Autowired private LockingTaskExecutor executor;

    @Test
    void shouldRunOnlyOnce_whenCalledConcurrently() throws Exception {
        AtomicInteger runCount = new AtomicInteger();
        List<Thread> threads = IntStream.range(0, 5)
            .mapToObj(i -> new Thread(() ->
                executor.executeWithLock(
                    runCount::incrementAndGet,
                    new LockConfiguration(Instant.now(), "testJob",
                        Duration.ofMinutes(1), Duration.ofSeconds(10)))))
            .collect(Collectors.toList());

        threads.forEach(Thread::start);
        for (Thread t : threads) t.join();

        assertEquals(1, runCount.get()); // only ONE execution
    }
}
```

---

## 7. Slow Downstream Affects Entire Platform

**Question:** One slow downstream service starts affecting the entire platform.

### Explanation

This is the **cascading failure** or **thread pool exhaustion** problem. When Service A calls slow Service B, threads in A's pool wait for B's response. With enough concurrent requests, A's thread pool fills up, and now A becomes slow for callers too — even if they don't touch B at all. The fix is **bulkhead isolation** (separate thread pools per downstream), **timeouts** on every HTTP call, and **circuit breakers** to stop calling a failing service.

### Key Points

- Thread pool exhaustion cascades upstream across services
- Bulkhead: separate thread pool per downstream dependency
- Timeout every HTTP call — never wait indefinitely
- Circuit breaker opens after threshold failures — fast-fails
- Fallback responses let the rest of the platform function

### Code Example

```java
// 1. Resilience4j Bulkhead — isolate downstream calls
@Configuration
public class ResilienceConfig {

    @Bean
    public BulkheadConfig inventoryBulkheadConfig() {
        return BulkheadConfig.custom()
            .maxConcurrentCalls(10)         // max 10 concurrent calls to Inventory
            .maxWaitDuration(Duration.ofMillis(100)) // queue wait limit
            .build();
    }

    @Bean
    public TimeLimiterConfig timeLimiterConfig() {
        return TimeLimiterConfig.custom()
            .timeoutDuration(Duration.ofSeconds(2)) // hard timeout per call
            .build();
    }
}

// 2. Apply bulkhead + circuit breaker + timeout
@Service
public class InventoryClient {

    private final Bulkhead bulkhead;
    private final CircuitBreaker circuitBreaker;
    private final RestTemplate restTemplate;

    public InventoryResponse getStock(String sku) {
        Supplier<InventoryResponse> decoratedCall = Decorators
            .ofSupplier(() -> restTemplate.getForObject(
                "http://inventory-service/stock/{sku}",
                InventoryResponse.class, sku))
            .withBulkhead(bulkhead)        // limit concurrency
            .withCircuitBreaker(circuitBreaker)   // stop on failures
            .withFallback(List.of(
                BulkheadFullException.class,
                CallNotPermittedException.class,  // circuit open
                ResourceAccessException.class),   // timeout
                ex -> fallbackResponse(sku, ex))
            .decorate();

        return decoratedCall.get();
    }

    private InventoryResponse fallbackResponse(String sku, Throwable ex) {
        log.warn("Inventory unavailable for SKU {}: {}", sku, ex.getMessage());
        // Return cached or default — platform stays functional
        return InventoryResponse.builder()
            .sku(sku)
            .available(true)   // optimistic default
            .source("FALLBACK")
            .build();
    }
}

// 3. Thread pool bulkhead (stronger isolation than semaphore)
@Bean
public ThreadPoolBulkhead inventoryThreadPoolBulkhead() {
    return ThreadPoolBulkhead.of("inventory",
        ThreadPoolBulkheadConfig.custom()
            .maxThreadPoolSize(10)
            .coreThreadPoolSize(5)
            .queueCapacity(20)    // reject beyond 20 queued requests
            .build());
}
```

---

## 8. Random 500 Errors, Infrastructure Healthy

**Question:** APIs randomly return 500 errors, but infrastructure looks healthy.

### Explanation

Random 500s with healthy infra usually point to **race conditions**, **unhandled edge cases in data**, **transient external service failures**, or **memory corruption** in shared mutable state. The errors are hard to reproduce because they depend on timing or specific data. Fix strategy: add structured logging with correlation IDs on every request, capture full stack traces, add global exception handlers, and introduce chaos testing to surface the root cause systematically.

### Key Points

- Race conditions in shared mutable state
- Unhandled null / edge-case data from specific users
- Transient downstream failures not caught and converted to 500
- Thread-local state leaking between requests in a thread pool
- Missing global exception handler — framework default = 500

### Code Example

```java
// 1. Global exception handler — never let raw exceptions become 500s
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpected(
            Exception ex, HttpServletRequest request) {

        String correlationId = MDC.get("correlationId");
        log.error("Unhandled exception [{}] on {} {}: {}",
            correlationId, request.getMethod(),
            request.getRequestURI(), ex.getMessage(), ex);

        // Categorize — don't expose internals to callers
        if (ex instanceof EntityNotFoundException) {
            return ResponseEntity.status(404)
                .body(new ErrorResponse("NOT_FOUND", ex.getMessage(), correlationId));
        }
        if (ex instanceof DataIntegrityViolationException) {
            return ResponseEntity.status(409)
                .body(new ErrorResponse("CONFLICT", "Resource conflict", correlationId));
        }
        // Generic fallback
        return ResponseEntity.status(500)
            .body(new ErrorResponse("INTERNAL_ERROR",
                "An unexpected error occurred", correlationId));
    }
}

// 2. Correlation ID filter — trace every request across logs
@Component
@Order(1)
public class CorrelationIdFilter extends OncePerRequestFilter {

    private static final String HEADER = "X-Correlation-ID";

    @Override
    protected void doFilterInternal(HttpServletRequest req,
            HttpServletResponse res, FilterChain chain)
            throws ServletException, IOException {

        String correlationId = Optional
            .ofNullable(req.getHeader(HEADER))
            .orElse(UUID.randomUUID().toString());

        MDC.put("correlationId", correlationId);
        res.setHeader(HEADER, correlationId);   // echo back to caller
        try {
            chain.doFilter(req, res);
        } finally {
            MDC.clear();   // CRITICAL: clear MDC for thread pool reuse
        }
    }
}

// 3. Avoid shared mutable state — use immutable or ThreadLocal carefully
// BAD: static mutable state — race condition guaranteed
public class FormatterBad {
    private static final SimpleDateFormat SDF =
        new SimpleDateFormat("yyyy-MM-dd"); // NOT thread-safe!
}

// GOOD: use DateTimeFormatter (thread-safe) or ThreadLocal
public class FormatterGood {
    private static final DateTimeFormatter DTF =
        DateTimeFormatter.ofPattern("yyyy-MM-dd"); // immutable, thread-safe

    // Or, if you must use legacy API:
    private static final ThreadLocal<SimpleDateFormat> TL_SDF =
        ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd"));
}
```

---

## 9. Health Checks Pass but Users Fail

**Question:** Health checks pass, but users still face failures.

### Explanation

Health checks that only ping `/actuator/health` (which checks if the JVM is running and the DB is reachable) give a false sense of safety. They don't validate **business logic correctness**, **downstream service health**, **data integrity**, or **degraded performance**. The gap: your pod is "up" but a misconfigured feature flag disables checkout, or a downstream service is partially broken. Implement **deep health checks** and **synthetic monitoring** that exercise real user flows.

### Key Points

- Shallow health checks: only verify 'is the process alive?'
- Business feature flags may be off even if DB is reachable
- Downstream partial failures not reflected in health status
- Readiness vs Liveness — different semantics, often confused
- Synthetic monitoring: run a real user flow every minute

### Code Example

```java
// 1. Custom deep health indicator — checks real business capability
@Component
public class CheckoutHealthIndicator implements HealthIndicator {

    private final ProductRepository productRepo;
    private final PaymentGateway gateway;
    private final FeatureFlagService featureFlags;

    @Override
    public Health health() {
        Map<String, Object> details = new LinkedHashMap<>();

        // Check 1: Can we read product data?
        try {
            long count = productRepo.countAvailableProducts();
            details.put("products_available", count);
            if (count == 0) {
                return Health.down()
                    .withDetails(details)
                    .withDetail("reason", "No products available in DB")
                    .build();
            }
        } catch (Exception e) {
            return Health.down(e).withDetails(details).build();
        }

        // Check 2: Is checkout feature enabled?
        if (!featureFlags.isEnabled("checkout")) {
            return Health.outOfService()
                .withDetails(details)
                .withDetail("reason", "Checkout feature flag is OFF")
                .build();
        }

        // Check 3: Can payment gateway be reached?
        if (!gateway.ping()) {
            return Health.down()
                .withDetails(details)
                .withDetail("reason", "Payment gateway unreachable")
                .build();
        }

        details.put("status", "All systems operational");
        return Health.up().withDetails(details).build();
    }
}

// 2. Separate Readiness and Liveness probes
// application.yml:
// management:
//   endpoint:
//     health:
//       group:
//         liveness:
//           include: livenessState        # just: is JVM alive?
//         readiness:
//           include: readinessState, db, checkout  # can we serve traffic?

// 3. Synthetic monitoring — test a real checkout flow every minute
@Scheduled(fixedDelay = 60_000)
public void syntheticCheckout() {
    try {
        // Use a dedicated test product and test user
        CartRequest cart = new CartRequest(TEST_PRODUCT_ID, 1, TEST_USER_ID);
        CheckoutResponse resp = checkoutService.checkout(cart);

        if (!resp.isSuccess()) {
            alertService.page("SYNTHETIC_CHECKOUT_FAILED: " + resp.getError());
        }
        meterRegistry.counter("synthetic.checkout.success").increment();
    } catch (Exception e) {
        meterRegistry.counter("synthetic.checkout.failure").increment();
        alertService.page("Synthetic checkout exception: " + e.getMessage());
    }
}
```

---

## 10. Cache Returns Stale Data

**Question:** Cache improves performance initially, but later starts returning stale data.

### Explanation

Caches become stale when the source data changes but the cache entry isn't invalidated or hasn't expired. Common pitfalls: TTL too long, cache-aside pattern with no write-through, multiple service instances with local caches out of sync, and missing cache versioning after a data schema change. A robust strategy: use **cache-aside with write-through**, choose TTL carefully per data sensitivity, version cache keys on schema changes, and use Redis pub/sub for distributed invalidation.

### Key Points

- TTL too long — data changes but cache holds old value
- Write-through missing — DB updated but cache not invalidated
- Local in-memory caches on multiple pods go out of sync
- Cache key doesn't include version — stale on schema change
- No cache stampede protection — thundering herd on expiry

### Code Example

```java
// 1. Spring Cache with Redis — write-through pattern
@Service
public class ProductService {

    @Cacheable(value = "products", key = "#id",
               unless = "#result == null")
    public Product getProduct(Long id) {
        return productRepo.findById(id).orElse(null);
    }

    @CachePut(value = "products", key = "#product.id")  // update cache on write
    @Transactional
    public Product updateProduct(Product product) {
        return productRepo.save(product);  // DB + cache updated atomically
    }

    @CacheEvict(value = "products", key = "#id")        // evict on delete
    @Transactional
    public void deleteProduct(Long id) {
        productRepo.deleteById(id);
    }
}

// 2. Distributed cache invalidation via Redis pub/sub
@Component
public class CacheInvalidationPublisher {

    private final RedisTemplate<String, String> redis;

    public void invalidate(String cacheKey) {
        redis.convertAndSend("cache-invalidation", cacheKey);
        log.info("Published cache invalidation for key: {}", cacheKey);
    }
}

@Component
public class CacheInvalidationListener implements MessageListener {

    private final CacheManager cacheManager;

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String cacheKey = new String(message.getBody());
        // All pods receive this and evict their local/Redis cache
        cacheManager.getCache("products").evict(cacheKey);
        log.info("Invalidated cache key: {}", cacheKey);
    }
}

// 3. Cache stampede prevention with probabilistic early expiration
@Service
public class CacheService {

    private final RedisTemplate<String, Object> redis;
    private final Random random = new Random();

    public <T> T getOrLoad(String key, Supplier<T> loader,
                           Duration ttl, double beta) {
        Object cached = redis.opsForValue().get(key);
        Long remainingTtl = redis.getExpire(key, TimeUnit.SECONDS);

        // Probabilistic early refresh: refresh before TTL expires
        if (cached == null || shouldRefreshEarly(remainingTtl, beta)) {
            T fresh = loader.get();
            redis.opsForValue().set(key, fresh, ttl);
            return fresh;
        }
        return (T) cached;
    }

    private boolean shouldRefreshEarly(Long ttlSeconds, double beta) {
        if (ttlSeconds == null || ttlSeconds > 30) return false;
        // Higher beta = more aggressive early refresh
        return -beta * Math.log(random.nextDouble()) >= ttlSeconds;
    }
}
```

---

## 11. Debugging Across Services is Difficult

**Question:** Logs exist everywhere, but debugging across services is still difficult.

### Explanation

In microservices, a single user request can touch 10+ services. Without **distributed tracing**, each service logs in isolation and there's no way to correlate what happened for a specific request. The solution stack: **structured JSON logs** with a `traceId` propagated through headers, **OpenTelemetry** for automatic instrumentation, and a **log aggregation** platform (ELK, Loki, Datadog) where you can search by traceId and see the full request journey in one view.

### Key Points

- No correlation ID → logs are islands, impossible to connect
- Structured JSON logs are machine-searchable; plain text isn't
- OpenTelemetry auto-instruments Spring Boot with zero code changes
- Propagate traceId in all HTTP headers and Kafka message headers
- Centralized log platform: search by traceId = full journey

### Code Example

```java
// 1. Propagate trace context via HTTP headers
@Component
public class TracingClientInterceptor implements ClientHttpRequestInterceptor {

    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body,
            ClientHttpRequestExecution exec) throws IOException {

        // Propagate OpenTelemetry W3C trace context
        Span currentSpan = Span.current();
        if (currentSpan.getSpanContext().isValid()) {
            request.getHeaders().add("traceparent",
                "00-" + currentSpan.getSpanContext().getTraceId()
                + "-" + currentSpan.getSpanContext().getSpanId() + "-01");
        }
        // Also propagate custom correlation ID from MDC
        String correlationId = MDC.get("correlationId");
        if (correlationId != null) {
            request.getHeaders().add("X-Correlation-ID", correlationId);
        }
        return exec.execute(request, body);
    }
}

// 2. Structured JSON logging with logback-spring.xml
// Every log line is a JSON object — searchable by any field
// {
//   "timestamp": "2024-01-15T10:30:00.123Z",
//   "level": "ERROR",
//   "service": "order-service",
//   "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
//   "spanId": "00f067aa0ba902b7",
//   "correlationId": "req-uuid-here",
//   "userId": "user-123",
//   "message": "Payment failed for order 456",
//   "exception": "..."
// }

// logback-spring.xml encoder:
// <encoder class="net.logstash.logback.encoder.LogstashEncoder">
//   <customFields>{"service":"order-service","env":"prod"}</customFields>
// </encoder>

// 3. Propagate trace through Kafka messages
@Component
public class TracingKafkaProducer {

    private final KafkaTemplate<String, String> kafka;

    public void sendWithTrace(String topic, String key, String value) {
        ProducerRecord<String, String> record =
            new ProducerRecord<>(topic, key, value);

        // Inject OpenTelemetry context into Kafka headers
        OpenTelemetry.getPropagators().getTextMapPropagator()
            .inject(Context.current(), record.headers(),
                (headers, k, v) -> headers.add(k, v.getBytes()));

        // Also add human-readable correlation ID
        record.headers().add("X-Correlation-ID",
            MDC.get("correlationId").getBytes());

        kafka.send(record);
    }
}

// 4. Extract trace on the consumer side
@KafkaListener(topics = "orders")
public void consume(ConsumerRecord<String, String> record) {
    // Restore trace context from headers
    Context extractedContext = OpenTelemetry.getPropagators()
        .getTextMapPropagator()
        .extract(Context.current(), record.headers(),
            (headers, key) -> {
                Header h = headers.lastHeader(key);
                return h != null ? new String(h.value()) : null;
            });

    try (Scope scope = extractedContext.makeCurrent()) {
        // All logs inside here share the same traceId as the producer
        log.info("Processing message — trace continues from producer");
        processOrder(record.value());
    }
}
```

---

## 12. JVM Memory Slowly Increases After Deployment

**Question:** JVM memory usage slowly increases after every deployment.

### Explanation

A slow, steady memory increase that never plateaus is a **memory leak** — objects are being created but never garbage collected because something holds a reference to them. Common culprits: static collections that grow unboundedly, event listeners never removed, ThreadLocal variables not cleaned up, third-party library caches, or metaspace growth from classloader leaks. Use **heap dumps** and a profiler (VisualVM, YourKit, Eclipse MAT) to find the object type that's accumulating.

### Key Points

- Static collections (Map, List) growing without eviction
- Event listeners registered but never deregistered
- ThreadLocal not removed after request → retained by thread pool
- Metaspace leak: classloaders not GC'd (dynamic class generation)
- Use heap dumps + Eclipse MAT to find the accumulating type

### Code Example

```java
// 1. Static cache without eviction — classic memory leak
// BAD: grows forever, never GC'd
public class CacheBad {
    private static final Map<String, byte[]> cache = new HashMap<>();

    public byte[] get(String key) {
        return cache.computeIfAbsent(key, k -> loadLargeData(k));
    }
}

// GOOD: bounded LRU cache with expiry
public class CacheGood {
    // Caffeine cache: max 1000 entries, expire 10 min after last access
    private static final Cache<String, byte[]> cache = Caffeine.newBuilder()
        .maximumSize(1000)
        .expireAfterAccess(Duration.ofMinutes(10))
        .recordStats()   // expose hit/miss metrics
        .build();

    public byte[] get(String key) {
        return cache.get(key, k -> loadLargeData(k));
    }
}

// 2. ThreadLocal leak — MUST remove after each request
@Component
public class UserContextFilter extends OncePerRequestFilter {

    // ThreadLocal can hold large objects — thread pool never discards threads
    private static final ThreadLocal<UserContext> USER_CONTEXT =
        new ThreadLocal<>();

    public static UserContext get() { return USER_CONTEXT.get(); }

    @Override
    protected void doFilterInternal(HttpServletRequest req,
            HttpServletResponse res, FilterChain chain)
            throws ServletException, IOException {
        try {
            USER_CONTEXT.set(resolveUser(req));
            chain.doFilter(req, res);
        } finally {
            USER_CONTEXT.remove();  // CRITICAL: prevents leak in thread pool
        }
    }
}

// 3. Heap dump analysis automation
@Scheduled(fixedDelay = 60_000)
public void checkHeapUsage() {
    MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
    long usedHeap  = memBean.getHeapMemoryUsage().getUsed();
    long maxHeap   = memBean.getHeapMemoryUsage().getMax();
    double pct     = (double) usedHeap / maxHeap * 100;

    log.info("Heap usage: {}% ({} MB / {} MB)",
        String.format("%.1f", pct),
        usedHeap  / 1_048_576,
        maxHeap   / 1_048_576);

    if (pct > 85) {
        log.error("HEAP CRITICAL: triggering heap dump for analysis");
        // Trigger via JMX or: jcmd <pid> GC.heap_dump /tmp/heap.hprof
        alertService.page("Heap > 85% on " + System.getenv("HOSTNAME"));
    }
}
```

---

## 13. APIs Work in Staging but Fail in Production Gateway

**Question:** APIs work in staging but fail behind the production gateway.

### Explanation

The production API gateway introduces layers that staging often doesn't replicate: **SSL termination**, **request/response size limits**, **timeout policies**, **authentication middleware**, **rate limiting**, and **header rewrites**. A common trap is that the service works fine when called directly but breaks when the gateway strips headers, enforces a 30-second timeout, or rejects large payloads. Always test through the full production network path.

### Key Points

- Gateway has a shorter timeout than the service's actual response time
- Request body size limit in the gateway (default: 1 MB in Nginx)
- Authentication headers stripped or rewritten by gateway
- Rate limiting kicks in under load — not visible in staging
- SSL certificate differences between staging and production

### Code Example

```java
// 1. Detect gateway timeout mismatches
// application.yml — align service timeout with gateway timeout
server:
  tomcat:
    connection-timeout: 25s  # MUST be < gateway timeout (e.g. 30 s)

spring:
  mvc:
    async:
      request-timeout: 25000  # async endpoint timeout

// 2. Log original client IP through gateway headers
@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Bean
    public FilterRegistrationBean<ForwardedHeaderFilter> forwardedHeaderFilter() {
        FilterRegistrationBean<ForwardedHeaderFilter> bean =
            new FilterRegistrationBean<>(new ForwardedHeaderFilter());
        bean.setOrder(0);
        return bean;
    }
}

// 3. Handle large payloads — configure limits explicitly
@Configuration
public class MultipartConfig {

    @Bean
    public MultipartConfigElement multipartConfig() {
        MultipartConfigFactory factory = new MultipartConfigFactory();
        factory.setMaxFileSize(DataSize.ofMegabytes(50));      // per file
        factory.setMaxRequestSize(DataSize.ofMegabytes(100));  // total
        return factory.createMultipartConfig();
    }
}

// 4. Test ALL gateway middleware in integration tests
@SpringBootTest(webEnvironment = RANDOM_PORT)
@TestPropertySource(properties = {
    "gateway.auth.enabled=true",    // same as prod
    "gateway.rate-limit=100"        // same as prod
})
class GatewayIntegrationTest {

    @Test
    void shouldHandleGatewayAuthHeader() {
        // Simulate what the gateway injects after validating JWT
        ResponseEntity<String> response = restTemplate.exchange(
            "/api/orders",
            HttpMethod.GET,
            new HttpEntity<>(buildGatewayHeaders()),  // X-User-ID, X-Roles, etc.
            String.class
        );
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    private HttpHeaders buildGatewayHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.add("X-User-ID", "test-user-123");
        headers.add("X-Roles", "ROLE_USER");
        headers.add("X-Forwarded-For", "10.0.0.1");
        headers.add("X-Request-ID", UUID.randomUUID().toString());
        return headers;
    }
}
```

---

## 14. Thread Pools Exhausted, CPU Stable

**Question:** Thread pools become exhausted even though CPU usage is stable.

### Explanation

CPU being stable while threads are exhausted is a clear sign of **blocking I/O** — threads are waiting (for DB, network, disk) rather than executing CPU instructions. Each waiting thread consumes memory (~1 MB stack) and a slot in the pool but burns no CPU. Under high concurrency this exhausts the pool. Fix: use **virtual threads** (Java 21+), **reactive programming** (WebFlux), or **async/non-blocking I/O** to stop tying threads to I/O waits.

### Key Points

- I/O-bound threads wait for DB/network — CPU stays idle
- Each blocked thread holds ~1 MB stack — memory pressure
- Virtual threads (Java 21): M:N mapping — millions of tasks
- Reactive WebFlux: non-blocking event loop — no thread-per-request
- Never call blocking APIs from reactive/virtual thread context

### Code Example

```java
// 1. Java 21 Virtual Threads — drop-in replacement
// application.properties:
// spring.threads.virtual.enabled=true  ← ONE LINE to enable in Spring Boot 3.2+

// Or configure explicitly:
@Bean
public TomcatProtocolHandlerCustomizer<?> virtualThreadCustomizer() {
    return handler -> handler.setExecutor(
        Executors.newVirtualThreadPerTaskExecutor()
    );
}

// 2. Async REST controller — release threads during I/O
@RestController
public class OrderController {

    // BAD: thread blocks for the full duration of DB + payment call
    @GetMapping("/orders/{id}/bad")
    public OrderDetail getOrderBad(@PathVariable Long id) {
        Order order = orderRepo.findById(id).orElseThrow(); // blocks ~10 ms
        Payment payment = paymentRepo.findByOrderId(id).orElseThrow(); // blocks ~10 ms
        return new OrderDetail(order, payment);
    }

    // GOOD: CompletableFuture — thread free during I/O
    @GetMapping("/orders/{id}/good")
    public CompletableFuture<OrderDetail> getOrderGood(@PathVariable Long id) {
        CompletableFuture<Order> orderFuture =
            CompletableFuture.supplyAsync(() ->
                orderRepo.findById(id).orElseThrow(), dbExecutor);

        CompletableFuture<Payment> paymentFuture =
            CompletableFuture.supplyAsync(() ->
                paymentRepo.findByOrderId(id).orElseThrow(), dbExecutor);

        // Both DB calls in parallel — total wait = max(t1, t2) not t1+t2
        return orderFuture.thenCombine(paymentFuture, OrderDetail::new);
    }
}

// 3. Thread pool metrics — catch exhaustion before it causes errors
@Scheduled(fixedDelay = 5_000)
public void monitorThreadPool() {
    ThreadPoolExecutor executor = (ThreadPoolExecutor) taskExecutor.getThreadPoolExecutor();

    int active    = executor.getActiveCount();
    int poolSize  = executor.getPoolSize();
    int queued    = executor.getQueue().size();
    long rejected = executor.getTaskCount()
                  - executor.getCompletedTaskCount() - active;

    log.info("Thread pool — active: {}/{}, queued: {}", active, poolSize, queued);

    if ((double) active / executor.getMaximumPoolSize() > 0.8) {
        log.warn("Thread pool nearing exhaustion: {}% utilized",
            active * 100 / executor.getMaximumPoolSize());
    }
}
```

---

## 15. Circuit Breakers Configured but Cascading Failures Still Happen

**Question:** Circuit breakers are configured, but cascading failures still happen.

### Explanation

Having a circuit breaker library doesn't mean it's working correctly. Common misconfigurations: thresholds too high (opens only after 50 failures, but by then damage is done), wrong exception types configured (timeouts not counted as failures), circuit breaker wrapping the wrong method (not the actual HTTP call), or the fallback itself being slow/failing. Verify with **chaos engineering** — deliberately break a dependency and confirm the circuit opens, the fallback fires, and the rest of the system stays healthy.

### Key Points

- Threshold too high — 50 failures before opening is too late
- Timeout exceptions not counted as failures in CB config
- CB wraps the service bean, not the actual outbound HTTP call
- Fallback method is itself slow or calls another failing service
- Missing chaos tests — you don't know if CB actually works

### Code Example

```java
// 1. Correct Resilience4j circuit breaker configuration
@Bean
public CircuitBreakerConfig paymentCircuitBreakerConfig() {
    return CircuitBreakerConfig.custom()
        // Open after 50% failure rate (not a raw count)
        .failureRateThreshold(50)
        // Minimum calls before evaluating failure rate
        .minimumNumberOfCalls(10)
        // Sliding window: last 20 calls
        .slidingWindowSize(20)
        .slidingWindowType(SlidingWindowType.COUNT_BASED)
        // How long to stay OPEN before trying HALF_OPEN
        .waitDurationInOpenState(Duration.ofSeconds(30))
        // How many test calls in HALF_OPEN state
        .permittedNumberOfCallsInHalfOpenState(5)
        // CRITICAL: treat slow calls as failures too
        .slowCallDurationThreshold(Duration.ofSeconds(2))
        .slowCallRateThreshold(50)
        // CRITICAL: include timeout exceptions
        .recordExceptions(
            IOException.class,
            TimeoutException.class,
            ResourceAccessException.class,
            ServiceUnavailableException.class
        )
        // Don't count business-logic exceptions as failures
        .ignoreExceptions(
            OrderNotFoundException.class,
            ValidationException.class
        )
        .build();
}

// 2. Apply correctly — wrap the OUTBOUND call, not the service class
@Service
public class PaymentGatewayClient {

    private final CircuitBreaker cb;
    private final RestTemplate restTemplate;

    public PaymentResult charge(PaymentRequest req) {
        // Correct: wrap the actual HTTP call
        return CircuitBreaker.decorateSupplier(cb,
            () -> restTemplate.postForObject(
                "http://payment-gateway/v1/charge",
                req, PaymentResult.class)
        ).get();

        // WRONG: wrapping a service method that calls 5 other things
        // cb.executeSupplier(() -> paymentService.processFullWorkflow(req));
    }
}

// 3. Fallback that CANNOT fail (self-contained)
@Service
public class PaymentServiceWithFallback {

    @CircuitBreaker(name = "payment", fallbackMethod = "chargeFallback")
    public PaymentResult charge(PaymentRequest req) {
        return gatewayClient.charge(req);
    }

    // Fallback: local-only, no external calls, guaranteed fast
    private PaymentResult chargeFallback(PaymentRequest req, Throwable ex) {
        log.error("Circuit breaker OPEN — payment gateway unavailable: {}",
                  ex.getMessage());

        // Queue for async retry via outbox — don't call another service!
        outboxRepo.save(OutboxEvent.builder()
            .type("PAYMENT_RETRY")
            .payload(serialize(req))
            .scheduledAt(Instant.now().plusSeconds(60))
            .build());

        return PaymentResult.queued("Payment queued — will retry shortly");
    }
}

// 4. Chaos test — verify the circuit breaker actually works
@SpringBootTest
class CircuitBreakerChaosTest {

    @Autowired private PaymentServiceWithFallback paymentService;
    @MockBean  private PaymentGatewayClient gatewayClient;

    @Test
    void shouldOpenCircuitAfterFailures_andServeFallback() {
        // Simulate gateway down
        when(gatewayClient.charge(any()))
            .thenThrow(new ResourceAccessException("Gateway down"));

        PaymentRequest req = new PaymentRequest("order-1", 100.0);

        // Fire enough calls to open the circuit
        IntStream.range(0, 15).forEach(i -> {
            PaymentResult result = paymentService.charge(req);
            // After threshold, should return QUEUED from fallback
            assertThat(result.getStatus()).isIn("FAILED", "QUEUED");
        });

        // Verify circuit is now OPEN — no more gateway calls
        verify(gatewayClient, atMost(12)).charge(any()); // stopped calling
    }
}
```

---
