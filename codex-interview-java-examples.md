# Java, Microservices, Kafka, Kubernetes, and AWS Interview Notes with Java Examples

## Java

### 1. How does ConcurrentHashMap achieve thread safety?

`ConcurrentHashMap` avoids one global lock. Reads are mostly lock-free through volatile reads. Updates use CAS where possible and synchronize only on the affected bucket/bin when contention occurs.

```java
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class SafeCounter {
    private final ConcurrentMap<String, Integer> counts = new ConcurrentHashMap<>();

    void increment(String key) {
        counts.merge(key, 1, Integer::sum);
    }

    int get(String key) {
        return counts.getOrDefault(key, 0);
    }
}
```

### 2. Virtual Threads vs Platform Threads

Platform threads are OS-backed and expensive at very high counts. Virtual threads are lightweight JVM-managed threads, best for blocking I/O workloads. They do not make CPU-bound code faster.

```java
import java.util.concurrent.Executors;

class VirtualThreadExample {
    public static void main(String[] args) throws Exception {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 10_000; i++) {
                int id = i;
                executor.submit(() -> {
                    Thread.sleep(100);
                    return "done-" + id;
                });
            }
        }
    }
}
```

### 3. How do you diagnose high CPU usage in a Java application?

Find the hot process, identify hot threads, map OS thread IDs to Java thread dump IDs, then profile with JFR or async-profiler. In code, high CPU often comes from tight loops, contention, retries, or excessive allocation.

```java
class CpuBug {
    volatile boolean running = true;

    void badLoop() {
        while (running) {
            // Busy spin: burns CPU.
        }
    }

    void betterLoop() throws InterruptedException {
        while (running) {
            Thread.sleep(10);
        }
    }
}
```

### 4. How do you identify and fix memory leaks?

A leak means objects remain reachable even though they are no longer useful. Use heap dumps, allocation profiling, dominator trees, and retained-size analysis. Common causes are unbounded maps, listeners, static references, and `ThreadLocal`.

```java
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class LeakyCache {
    private final Map<String, byte[]> cache = new ConcurrentHashMap<>();

    void put(String key, byte[] data) {
        cache.put(key, data); // No size or TTL limit.
    }
}

class FixedCache {
    private final Map<String, byte[]> cache = new java.util.LinkedHashMap<>() {
        protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
            return size() > 1_000;
        }
    };
}
```

### 5. Explain G1 GC, ZGC, and Shenandoah. When would you choose each?

G1 is the balanced default for many services. ZGC and Shenandoah are low-latency collectors that do most work concurrently and target very short pauses.

```java
class GcSensitiveService {
    // Throughput-friendly: reduce allocation rate.
    String normalize(String input) {
        return input == null ? "" : input.trim().toLowerCase();
    }

    // GC-unfriendly if called heavily: creates many temporary strings.
    String noisy(String input) {
        return new StringBuilder()
                .append("prefix-")
                .append(input.trim().toLowerCase())
                .append("-suffix")
                .toString();
    }
}
```

Choose G1 for general services, ZGC for very large heaps or strict latency, and Shenandoah when you want low pauses in environments where it is the preferred supported collector.

### 6. What happens internally when a Full GC occurs?

A Full GC usually stops application threads, marks live objects, may unload classes, compacts memory, updates references, and resumes the application. It is expensive because it inspects a broad memory area.

```java
class FullGcRisk {
    static final java.util.List<byte[]> retained = new java.util.ArrayList<>();

    static void createPressure() {
        for (int i = 0; i < 1_000; i++) {
            retained.add(new byte[1024 * 1024]);
        }
    }
}
```

### 7. How do you optimize a high-throughput Java application?

Measure first. Reduce allocation, batch I/O, remove lock contention, tune thread pools, use efficient serialization, optimize database calls, and tune the JVM only after fixing application bottlenecks.

```java
import java.util.ArrayList;
import java.util.List;

class BatchWriter {
    void writeBatch(List<String> events) {
        // One database or network call for many events is usually better.
    }

    void process(List<String> events) {
        List<String> batch = new ArrayList<>(500);
        for (String event : events) {
            batch.add(event);
            if (batch.size() == 500) {
                writeBatch(batch);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) writeBatch(batch);
    }
}
```

### 8. Explain Java Memory Model (JMM).

The JMM defines visibility and ordering between threads. `volatile`, `synchronized`, atomics, thread start/join, and final fields create happens-before relationships.

```java
class Visibility {
    private volatile boolean stopped;

    void stop() {
        stopped = true;
    }

    void run() {
        while (!stopped) {
            doWork();
        }
    }

    void doWork() {}
}
```

### 9. What causes thread starvation and deadlocks?

Starvation happens when a thread cannot get CPU, locks, or pool capacity. Deadlock happens when threads wait on each other in a cycle.

```java
class DeadlockExample {
    private final Object a = new Object();
    private final Object b = new Object();

    void first() {
        synchronized (a) {
            synchronized (b) {
                work();
            }
        }
    }

    void second() {
        synchronized (b) {
            synchronized (a) {
                work();
            }
        }
    }

    void work() {}
}
```

Fix by always acquiring locks in the same order or by using timeouts.

### 10. How does the ForkJoinPool work?

`ForkJoinPool` splits work into small tasks. Each worker owns a deque. Workers process their own tasks and steal tasks from others when idle.

```java
import java.util.concurrent.RecursiveTask;

class SumTask extends RecursiveTask<Long> {
    private final int[] values;
    private final int start;
    private final int end;

    SumTask(int[] values, int start, int end) {
        this.values = values;
        this.start = start;
        this.end = end;
    }

    protected Long compute() {
        if (end - start <= 1_000) {
            long sum = 0;
            for (int i = start; i < end; i++) sum += values[i];
            return sum;
        }
        int mid = (start + end) / 2;
        var left = new SumTask(values, start, mid);
        var right = new SumTask(values, mid, end);
        left.fork();
        return right.compute() + left.join();
    }
}
```

### 11. What is False Sharing and how can it impact performance?

False sharing happens when independent hot variables sit on the same CPU cache line. Different cores keep invalidating each other's cache line, slowing the program.

```java
class FalseSharingRisk {
    volatile long counter1;
    volatile long counter2; // May share cache line with counter1.
}

class PaddedCounters {
    volatile long counter1;
    long p1, p2, p3, p4, p5, p6, p7;
    volatile long counter2;
}
```

### 12. How would you profile a production JVM?

Use low-overhead tools first: JFR, metrics, GC logs, thread dumps, heap histograms, and async-profiler. Avoid invasive profilers during incidents.

```java
import jdk.jfr.Event;
import jdk.jfr.Label;

@Label("Payment Slow Path")
class PaymentSlowPathEvent extends Event {
    @Label("Order ID")
    String orderId;
}

class PaymentService {
    void pay(String orderId) {
        var event = new PaymentSlowPathEvent();
        event.orderId = orderId;
        event.begin();
        try {
            callBank();
        } finally {
            event.commit();
        }
    }

    void callBank() {}
}
```

### 13. What are the limitations of Virtual Threads?

They are excellent for blocking I/O, but not for CPU-heavy work. You still need backpressure. Large `ThreadLocal` usage can waste memory. Native or foreign calls may pin carrier threads, and older Java versions had more pinning around `synchronized`.

```java
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

class VirtualThreadBackpressure {
    private final Semaphore dbLimit = new Semaphore(50);

    void handleRequests() throws Exception {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 10_000; i++) {
                executor.submit(this::queryDatabaseSafely);
            }
        }
    }

    void queryDatabaseSafely() throws Exception {
        dbLimit.acquire();
        try {
            queryDatabase();
        } finally {
            dbLimit.release();
        }
    }

    void queryDatabase() {}
}
```

### 14. How does CompletableFuture work internally?

It stores a result, failure, or incomplete state. Dependent stages are registered as callbacks and executed when the future completes, either in the completing thread or in an executor.

```java
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class CompletableFutureExample {
    private final ExecutorService pool = Executors.newFixedThreadPool(8);

    CompletableFuture<String> loadUserName(String userId) {
        return CompletableFuture
                .supplyAsync(() -> loadUser(userId), pool)
                .thenApply(User::name)
                .exceptionally(ex -> "unknown");
    }

    User loadUser(String userId) {
        return new User("Asha");
    }

    record User(String name) {}
}
```

### 15. How would you design a thread-safe cache?

Use `ConcurrentHashMap`, atomic loading, TTL or size eviction, metrics, and protection against cache stampede. In production, prefer Caffeine.

```java
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

class SimpleTtlCache<K, V> {
    private final ConcurrentHashMap<K, Entry<V>> map = new ConcurrentHashMap<>();
    private final long ttlMillis;

    SimpleTtlCache(long ttlMillis) {
        this.ttlMillis = ttlMillis;
    }

    V get(K key, java.util.function.Function<K, V> loader) {
        long now = System.currentTimeMillis();
        Entry<V> entry = map.compute(key, (k, old) -> {
            if (old == null || old.expiresAt < now) {
                return new Entry<>(loader.apply(k), now + ttlMillis);
            }
            return old;
        });
        return entry.value;
    }

    record Entry<V>(V value, long expiresAt) {}
}
```

## Microservices

### 16. How would you design a fault-tolerant microservice architecture?

Use independent deployable services, timeouts, retries, circuit breakers, bulkheads, queues, health checks, autoscaling, observability, and graceful degradation.

```java
import java.net.http.HttpClient;
import java.time.Duration;

class FaultTolerantClient {
    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(500))
            .build();

    // Add retry/circuit breaker around outbound calls in real systems.
}
```

### 17. How do you prevent cascading failures?

Set timeouts, cap concurrency, shed load, use circuit breakers, isolate thread pools, and return fallbacks when dependencies fail.

```java
import java.util.concurrent.Semaphore;

class Bulkhead {
    private final Semaphore permits = new Semaphore(20);

    String callInventory() throws Exception {
        if (!permits.tryAcquire()) {
            return "inventory-temporarily-unavailable";
        }
        try {
            return remoteCall();
        } finally {
            permits.release();
        }
    }

    String remoteCall() {
        return "ok";
    }
}
```

### 18. Circuit Breaker vs Retry Pattern

Retry repeats a failed operation for transient errors. A circuit breaker stops calls when a dependency is unhealthy, giving it time to recover.

```java
class SimpleCircuitBreaker {
    private int failures;
    private long openedUntil;

    String call() {
        if (System.currentTimeMillis() < openedUntil) {
            return "fallback";
        }
        try {
            String result = remote();
            failures = 0;
            return result;
        } catch (RuntimeException ex) {
            if (++failures >= 3) {
                openedUntil = System.currentTimeMillis() + 10_000;
            }
            return "fallback";
        }
    }

    String remote() {
        throw new RuntimeException("down");
    }
}
```

### 19. How would you implement distributed transactions?

Prefer Saga and Outbox over two-phase commit. Commit local state and an event in the same database transaction, then publish the event asynchronously.

```java
class OrderService {
    void createOrder(Order order) {
        // In one database transaction:
        saveOrder(order);
        saveOutboxEvent(new OutboxEvent("OrderCreated", order.id()));
    }

    void saveOrder(Order order) {}
    void saveOutboxEvent(OutboxEvent event) {}

    record Order(String id) {}
    record OutboxEvent(String type, String aggregateId) {}
}
```

### 20. Saga Choreography vs Saga Orchestration

Choreography uses events and no central coordinator. Orchestration uses one workflow component that commands each step and handles compensation.

```java
class OrderSagaOrchestrator {
    void placeOrder(String orderId) {
        try {
            reserveInventory(orderId);
            chargePayment(orderId);
            ship(orderId);
        } catch (Exception ex) {
            cancelInventory(orderId);
            refundPayment(orderId);
        }
    }

    void reserveInventory(String id) {}
    void chargePayment(String id) {}
    void ship(String id) {}
    void cancelInventory(String id) {}
    void refundPayment(String id) {}
}
```

### 21. What are the drawbacks of microservices?

They add distributed failures, latency, operational complexity, data consistency challenges, testing complexity, versioning issues, and higher infrastructure cost.

```java
class DistributedCallRisk {
    OrderView getOrder(String orderId) {
        Order order = getOrderService(orderId);
        Payment payment = getPaymentService(orderId);
        Shipment shipment = getShipmentService(orderId);
        return new OrderView(order, payment, shipment);
    }

    Order getOrderService(String id) { return new Order(id); }
    Payment getPaymentService(String id) { return new Payment(id); }
    Shipment getShipmentService(String id) { return new Shipment(id); }

    record Order(String id) {}
    record Payment(String id) {}
    record Shipment(String id) {}
    record OrderView(Order order, Payment payment, Shipment shipment) {}
}
```

Each remote call can fail or become slow.

### 22. How do you handle service-to-service communication?

Use REST or gRPC for request/response and messaging for asynchronous workflows. Always include timeouts, retries, authentication, tracing, and versioning.

```java
import java.net.URI;
import java.net.http.HttpRequest;
import java.time.Duration;

class ServiceRequest {
    HttpRequest request(String traceId) {
        return HttpRequest.newBuilder()
                .uri(URI.create("https://inventory.internal/items/123"))
                .timeout(Duration.ofMillis(800))
                .header("X-Trace-Id", traceId)
                .GET()
                .build();
    }
}
```

### 23. REST vs gRPC in microservices?

REST is simple, cache-friendly, and widely supported. gRPC is strongly typed, faster for internal calls, supports streaming, and works well with generated clients.

```java
// REST-style DTO.
record CreateOrderRequest(String productId, int quantity) {}
record CreateOrderResponse(String orderId, String status) {}

// gRPC would define the same contract in a .proto file and generate Java classes.
```

### 24. How do you implement idempotency?

Use an idempotency key, store the result of the first successful request, and return the same result for duplicate requests.

```java
import java.util.concurrent.ConcurrentHashMap;

class IdempotencyService {
    private final ConcurrentHashMap<String, String> responses = new ConcurrentHashMap<>();

    String createPayment(String idempotencyKey) {
        return responses.computeIfAbsent(idempotencyKey, key -> chargeCard());
    }

    String chargeCard() {
        return "payment-123";
    }
}
```

### 25. How do you handle API versioning?

Prefer backward-compatible changes. For breaking changes, use explicit versions and deprecation windows.

```java
record UserV1(String id, String name) {}
record UserV2(String id, String firstName, String lastName) {}

class UserController {
    UserV1 getV1(String id) {
        return new UserV1(id, "Asha Rao");
    }

    UserV2 getV2(String id) {
        return new UserV2(id, "Asha", "Rao");
    }
}
```

### 26. How would you design a notification system?

Accept requests, persist them, enqueue work, send through channel workers, retry failures, use DLQs, apply user preferences and rate limits, and track delivery status.

```java
record Notification(String userId, String channel, String message) {}

class NotificationService {
    void notify(Notification notification) {
        save(notification);
        enqueue(notification);
    }

    void save(Notification notification) {}
    void enqueue(Notification notification) {}
}
```

### 27. How would you secure internal microservice communication?

Use mTLS, service identity, authorization, network policies, secret rotation, request signing if needed, and least privilege.

```java
import java.net.URI;
import java.net.http.HttpRequest;

class InternalAuth {
    HttpRequest signedRequest(String token) {
        return HttpRequest.newBuilder()
                .uri(URI.create("https://billing.internal/invoices"))
                .header("Authorization", "Bearer " + token)
                .header("X-Service-Name", "orders")
                .build();
    }
}
```

### 28. What happens when one microservice is unavailable?

Callers should timeout, retry only safe operations, open a circuit breaker, degrade gracefully, queue async work, and emit alerts.

```java
class FallbackExample {
    ProductDetails getProduct(String id) {
        try {
            return callCatalog(id);
        } catch (Exception ex) {
            return new ProductDetails(id, "Unknown", false);
        }
    }

    ProductDetails callCatalog(String id) {
        throw new RuntimeException("catalog down");
    }

    record ProductDetails(String id, String name, boolean fresh) {}
}
```

### 29. How do you handle eventual consistency?

Use events, outbox, idempotent consumers, reconciliation jobs, and user-visible pending states.

```java
class EventuallyConsistentOrder {
    OrderStatus placeOrder(String orderId) {
        saveOrder(orderId, "PENDING_PAYMENT");
        publishEvent("OrderPlaced", orderId);
        return new OrderStatus(orderId, "PENDING_PAYMENT");
    }

    void onPaymentCaptured(String orderId) {
        updateOrder(orderId, "CONFIRMED");
    }

    void saveOrder(String id, String status) {}
    void publishEvent(String type, String id) {}
    void updateOrder(String id, String status) {}

    record OrderStatus(String orderId, String status) {}
}
```

### 30. How would you debug a slow microservice?

Check latency percentiles, traces, dependency timings, database queries, thread pools, CPU, memory, GC, logs, saturation, and recent deployments.

```java
class TimingInterceptor {
    <T> T time(String operation, java.util.concurrent.Callable<T> callable) throws Exception {
        long start = System.nanoTime();
        try {
            return callable.call();
        } finally {
            long millis = (System.nanoTime() - start) / 1_000_000;
            System.out.println(operation + " took " + millis + " ms");
        }
    }
}
```

## Kafka

### 31. How does Kafka achieve high throughput?

Kafka uses append-only logs, sequential disk writes, batching, compression, page cache, zero-copy transfer, partitions, and consumer groups.

```java
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

class HighThroughputProducerConfig {
    Properties props() {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return props;
    }
}
```

### 32. What causes consumer lag?

Consumers lag when production exceeds consumption. Causes include slow processing, too few consumers, hot partitions, rebalances, downstream slowness, GC pauses, or broker/network issues.

```java
class ConsumerLagExample {
    void process(String message) throws InterruptedException {
        Thread.sleep(500); // Slow handler creates lag if messages arrive faster.
    }
}
```

### 33. How do you handle duplicate messages?

Make consumers idempotent. Store processed message IDs or use unique business keys.

```java
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class IdempotentConsumer {
    private final Set<String> processedIds = ConcurrentHashMap.newKeySet();

    void consume(String messageId, String payload) {
        if (!processedIds.add(messageId)) {
            return;
        }
        applyBusinessChange(payload);
    }

    void applyBusinessChange(String payload) {}
}
```

### 34. Explain Kafka partitioning strategy.

Messages with the same key go to the same partition, preserving order for that key. No key usually means load-balanced distribution.

```java
import org.apache.kafka.clients.producer.ProducerRecord;

class PartitioningExample {
    ProducerRecord<String, String> orderEvent(String orderId, String eventJson) {
        return new ProducerRecord<>("orders", orderId, eventJson);
    }
}
```

### 35. How do you guarantee message ordering?

Kafka guarantees ordering inside one partition. Use the same key for related events and process one partition sequentially.

```java
class OrderedOrderEvents {
    void publish(OrderEvent event) {
        String key = event.orderId();
        // ProducerRecord topic key ensures same orderId goes to same partition.
    }

    record OrderEvent(String orderId, String type) {}
}
```

### 36. What is exactly-once processing?

In Kafka, exactly-once combines idempotent producers and transactions so consumed offsets and produced output records are committed atomically. External systems still require idempotency.

```java
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

class TransactionalProducer {
    void send(KafkaProducer<String, String> producer, String key, String value) {
        producer.initTransactions();
        producer.beginTransaction();
        try {
            producer.send(new ProducerRecord<>("output", key, value));
            producer.commitTransaction();
        } catch (Exception ex) {
            producer.abortTransaction();
        }
    }
}
```

### 37. What happens when a Kafka broker fails?

If replicas are available, partition leadership moves to another in-sync replica. Clients refresh metadata and continue. Under-replicated partitions recover when brokers return.

```java
import org.apache.kafka.clients.producer.ProducerConfig;

class ReliableProducerConfig {
    Properties props() {
        var props = new Properties();
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        return props;
    }
}
```

### 38. How would you design a Dead Letter Queue strategy?

After limited retries, send failed records to a DLQ with original payload, headers, error reason, timestamp, and retry count. Alert and provide replay tooling.

```java
record DeadLetter(String originalTopic, String key, String payload, String reason) {}

class DlqHandler {
    DeadLetter toDlq(String topic, String key, String payload, Exception ex) {
        return new DeadLetter(topic, key, payload, ex.getMessage());
    }
}
```

### 39. Kafka vs RabbitMQ?

Kafka is a distributed event log for high-throughput streams and replay. RabbitMQ is a message broker optimized for routing, queues, and command-style messaging.

```java
class MessagingChoice {
    String choose(boolean needReplay, boolean needComplexRouting) {
        if (needReplay) return "Kafka";
        if (needComplexRouting) return "RabbitMQ";
        return "Depends on throughput, ordering, and operations model";
    }
}
```

### 40. How do you replay historical events safely?

Use a new consumer group or reset offsets, make handlers idempotent, throttle replay, isolate side effects, and test in staging first.

```java
class ReplaySafeHandler {
    void handle(Event event) {
        if (alreadyApplied(event.id())) {
            return;
        }
        apply(event);
        markApplied(event.id());
    }

    boolean alreadyApplied(String id) { return false; }
    void apply(Event event) {}
    void markApplied(String id) {}

    record Event(String id, String payload) {}
}
```

## Kubernetes

### 41. What happens when a Pod crashes?

The kubelet restarts crashed containers according to the restart policy. Controllers like Deployments create replacement Pods if necessary.

```java
class CrashExample {
    public static void main(String[] args) {
        throw new RuntimeException("Container exits; Kubernetes may restart it");
    }
}
```

### 42. Difference between Deployment, StatefulSet, and DaemonSet?

Deployment is for stateless replicas. StatefulSet gives stable identity and storage. DaemonSet runs one Pod per node, often for agents.

```java
class WorkloadChoice {
    String choose(boolean needsStableIdentity, boolean runOnEveryNode) {
        if (runOnEveryNode) return "DaemonSet";
        if (needsStableIdentity) return "StatefulSet";
        return "Deployment";
    }
}
```

### 43. How does Kubernetes Service Discovery work?

Services select ready Pods by labels and provide a stable DNS name and virtual IP. Java services normally call the Kubernetes DNS name.

```java
import java.net.URI;
import java.net.http.HttpRequest;

class KubernetesDnsClient {
    HttpRequest inventoryRequest() {
        return HttpRequest.newBuilder()
                .uri(URI.create("http://inventory.default.svc.cluster.local/items/123"))
                .GET()
                .build();
    }
}
```

### 44. What are Readiness and Liveness probes?

Readiness says whether the Pod should receive traffic. Liveness says whether the container should be restarted.

```java
class HealthController {
    String readiness() {
        return databaseReachable() ? "READY" : "NOT_READY";
    }

    String liveness() {
        return "ALIVE";
    }

    boolean databaseReachable() {
        return true;
    }
}
```

### 45. How do you troubleshoot a CrashLoopBackOff?

Check previous logs, events, image, command, env vars, secrets, config maps, startup time, probes, resource limits, and recent deploy changes.

```java
class StartupFailure {
    public static void main(String[] args) {
        String required = System.getenv("DATABASE_URL");
        if (required == null || required.isBlank()) {
            throw new IllegalStateException("DATABASE_URL is required");
        }
    }
}
```

### 46. What causes pod eviction?

Eviction can happen from memory, disk, or PID pressure; node drain; taints; priority/preemption; or exceeding ephemeral storage.

```java
import java.util.ArrayList;
import java.util.List;

class MemoryPressureRisk {
    static final List<byte[]> data = new ArrayList<>();

    public static void main(String[] args) {
        while (true) {
            data.add(new byte[10 * 1024 * 1024]);
        }
    }
}
```

### 47. HPA vs VPA?

HPA changes replica count. VPA changes CPU/memory requests. Use HPA for stateless traffic scaling and VPA for right-sizing workloads.

```java
class ScalingSignal {
    double cpuUtilization(long usedMillis, long requestedMillis) {
        return (double) usedMillis / requestedMillis;
    }
}
```

### 48. How would you scale a Java application in Kubernetes?

Use container-aware JVM settings, requests/limits, HPA based on CPU/RPS/queue lag, readiness probes, graceful shutdown, and startup probes for slow JVM boot.

```java
class GracefulShutdown {
    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Stop accepting traffic and finish in-flight work");
        }));
    }
}
```

### 49. How do you manage secrets securely?

Use Kubernetes Secrets with encryption at rest, external secret managers, RBAC, rotation, short-lived credentials, and never log secrets.

```java
class SecretUsage {
    void connect() {
        String password = System.getenv("DB_PASSWORD");
        if (password == null) throw new IllegalStateException("Missing secret");
        // Do not print the password.
    }
}
```

### 50. What happens during a rolling deployment?

Kubernetes creates new Pods, waits for readiness, gradually removes old Pods, respects surge/unavailable limits, and rolls back if configured checks fail.

```java
class VersionEndpoint {
    String version() {
        return System.getenv().getOrDefault("APP_VERSION", "unknown");
    }
}
```

## AWS

### 51. EC2 vs ECS vs EKS?

EC2 gives VM-level control. ECS runs containers with AWS-native orchestration. EKS runs managed Kubernetes.

```java
class ComputeChoice {
    String choose(boolean needKubernetes, boolean wantVmControl) {
        if (wantVmControl) return "EC2";
        if (needKubernetes) return "EKS";
        return "ECS";
    }
}
```

### 52. When would you choose Lambda over ECS?

Choose Lambda for event-driven, short-lived, spiky workloads with minimal operations. Choose ECS for long-running services, custom networking, or steady workloads.

```java
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

class OrderLambda implements RequestHandler<OrderEvent, String> {
    public String handleRequest(OrderEvent event, Context context) {
        return "processed-" + event.orderId();
    }

    record OrderEvent(String orderId) {}
}
```

### 53. SQS vs SNS?

SQS is a queue for decoupling producers and consumers. SNS is pub/sub fanout to multiple subscribers.

```java
class MessagingAwsChoice {
    String choose(boolean fanoutToManySubscribers) {
        return fanoutToManySubscribers ? "SNS" : "SQS";
    }
}
```

### 54. How does S3 achieve durability?

S3 stores objects redundantly across multiple devices and Availability Zones, checks integrity, repairs failures, and supports versioning and replication.

```java
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.file.Path;

class S3Upload {
    void upload(S3Client s3, String bucket, String key, Path file) {
        s3.putObject(PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build(), file);
    }
}
```

### 55. How do you secure applications in AWS?

Use IAM least privilege, VPC controls, security groups, KMS, Secrets Manager, CloudTrail, GuardDuty, WAF, patching, backups, and private networking where possible.

```java
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;

class AwsSecretReader {
    String readSecret(SecretsManagerClient client, String secretId) {
        return client.getSecretValue(GetSecretValueRequest.builder()
                .secretId(secretId)
                .build()).secretString();
    }
}
```

### 56. RDS vs DynamoDB?

RDS is relational and supports SQL, joins, and transactions. DynamoDB is a managed NoSQL key-value/document database designed for massive scale and predictable access patterns.

```java
class DatabaseChoice {
    String choose(boolean needsJoins, boolean predictableKeyValueAccess) {
        if (needsJoins) return "RDS";
        if (predictableKeyValueAccess) return "DynamoDB";
        return "Depends on data model and consistency needs";
    }
}
```

### 57. What is AWS Auto Scaling?

Auto Scaling adjusts capacity based on metrics or schedules, such as EC2 instances, ECS tasks, DynamoDB capacity, or EKS node groups.

```java
class ScalingPolicyDecision {
    int desiredCapacity(double cpuPercent, int current) {
        if (cpuPercent > 70) return current + 1;
        if (cpuPercent < 30 && current > 1) return current - 1;
        return current;
    }
}
```

### 58. How would you design a highly available application in AWS?

Use multiple Availability Zones, load balancers, stateless app replicas, autoscaling, managed databases with Multi-AZ or DynamoDB, queues, health checks, backups, and disaster recovery.

```java
class HealthCheck {
    String health() {
        return "OK";
    }

    String readiness() {
        return databaseAvailable() ? "READY" : "NOT_READY";
    }

    boolean databaseAvailable() {
        return true;
    }
}
```

### 59. What is the difference between ALB and NLB?

ALB is layer 7 and routes HTTP/HTTPS by host, path, and headers. NLB is layer 4 for TCP/UDP/TLS with very high performance and static IP support.

```java
class LoadBalancerChoice {
    String choose(boolean httpRoutingNeeded, boolean rawTcpNeeded) {
        if (httpRoutingNeeded) return "ALB";
        if (rawTcpNeeded) return "NLB";
        return "ALB for most HTTP services";
    }
}
```

### 60. How do you troubleshoot high latency in AWS?

Check client region, DNS, load balancer metrics, target health, app traces, DB latency, NAT gateways, throttling, retries, network paths, and CloudWatch metrics.

```java
class LatencyProbe {
    <T> T measure(String dependency, java.util.concurrent.Callable<T> call) throws Exception {
        long start = System.nanoTime();
        try {
            return call.call();
        } finally {
            long millis = (System.nanoTime() - start) / 1_000_000;
            System.out.println(dependency + " latency_ms=" + millis);
        }
    }
}
```

