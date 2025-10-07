# QLite

![Tests](https://github.com/umit/qlite/actions/workflows/tests.yml/badge.svg)

A high-performance, persistent job queue built on RocksDB with time-based scheduling and retry mechanisms.

## Features

- **Persistent Storage** - RocksDB-backed, survives restarts
- **Time-based Scheduling** - Schedule jobs for future execution
- **Automatic Retry** - Configurable retry logic with max attempts
- **Concurrent Workers** - Multiple workers can process jobs safely (single-node)
- **Multiple Serialization** - String, JSON, Kryo support
- **Standard Queue Interface** - Implements Java's `Queue<E>`

## Quick Start

### Maven Dependency

```xml
<dependency>
    <groupId>com.umitunal</groupId>
    <artifactId>qlite</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

### Basic Usage

```java
StorageConfig config = StorageConfig.newBuilder("/tmp/my-queue")
    .withDurableWrites(false)  // Fast writes
    .build();

try (JobQueue<String> queue = new RocksJobQueue<>(config, new StringCodec())) {
    // Enqueue a job
    long now = System.currentTimeMillis();
    queue.enqueue("job-1", "Process payment", now, 3);

    // Worker acquires and processes
    Job<String> job = queue.acquire("worker-1", 30000);
    if (job != null) {
        System.out.println("Processing: " + job.getPayload());
        queue.acknowledge(job);
    }
}
```

### Standard Queue Interface

```java
try (PersistentQueue<String> queue = new RocksPersistentQueue<>(
        config, new StringCodec(), "worker-1")) {

    queue.offer("Task 1");
    queue.offer("Task 2");

    String task = queue.poll();  // Returns "Task 1"
}
```

## Usage Examples

### Time-based Scheduling

```java
long now = System.currentTimeMillis();

// Execute immediately
queue.enqueue("email-1", emailData, now, 3);

// Execute in 1 hour
queue.enqueue("reminder-1", reminderData, now + 3600000, 3);
```

### Retry Mechanism

```java
Job<String> job = queue.acquire("worker-1", 30000);

try {
    processJob(job.getPayload());
    queue.acknowledge(job);  // Success
} catch (Exception e) {
    queue.reject(job, e.getMessage());  // Will retry if attempts < max
}
```

### Concurrent Workers

```java
QueueWorker<String> worker1 = QueueWorker.builder("worker-1", queue, processor)
    .withLeaseDuration(30000)
    .withPollInterval(1000)
    .build();

worker1.start();
```

### JSON Serialization

```java
public class EmailTask {
    private String recipient;
    private String subject;
    private String body;
}

JsonCodec<EmailTask> codec = new JsonCodec<>(EmailTask.class);
JobQueue<EmailTask> queue = new RocksJobQueue<>(config, codec);

EmailTask email = new EmailTask("user@example.com", "Welcome", "Hello!");
queue.enqueue("email-1", email, System.currentTimeMillis(), 3);
```

### Kryo Serialization (High Performance)

Kryo provides **5-10x faster** serialization than JSON with **2-3x smaller** payloads.

```java
KryoCodec<OrderEvent> codec = new KryoCodec<>(OrderEvent.class);
JobQueue<OrderEvent> queue = new RocksJobQueue<>(config, codec);

OrderEvent order = new OrderEvent("ORD-123", items, metadata);
queue.enqueue("order-1", order, System.currentTimeMillis(), 3);
```

## Configuration

```java
StorageConfig config = StorageConfig.newBuilder("/path/to/db")
    .withDurableWrites(true)       // fsync on every write
    .withMemoryBufferSize(64)      // 64MB write buffer
    .withMaxMemoryBuffers(3)       // Keep 3 buffers
    .withBackgroundThreads(4)      // Compaction threads
    .build();
```

## Monitoring

```java
QueueMetrics metrics = queue.getMetrics();

System.out.println("Total jobs: " + metrics.getTotalJobs());
System.out.println("Queued: " + metrics.getQueuedJobs());
System.out.println("Leased: " + metrics.getLeasedJobs());
System.out.println("Completed: " + metrics.getSuccessfulJobs());
System.out.println("Failed: " + metrics.getFailedJobs());
```

## Job States

- **QUEUED** - Waiting for execution
- **LEASED** - Claimed by a worker
- **SUCCESS** - Completed successfully
- **FAILED** - Failed after max retries
- **ABANDONED** - Lease expired without completion

## Performance

Benchmarked on **MacBook Pro M1, 16GB RAM**:

- **Enqueue**: ~100K ops/s
- **Acquire**: ~50K ops/s
- **Bloom filters** for fast point lookups
- **128MB LRU cache**
- **ByteBuffer-based serialization**

## Testing

```bash
mvn test
```

- **57 unit tests** (100% passing)
- JUnit 5 + AssertJ + Awaitility
- Core functionality and edge cases covered

## Limitations

### Single-Node Design

This queue is designed for **single-node concurrent processing**.

- ❌ **Not distributed** - Cannot share across multiple machines
- ✅ **Multi-threaded safe** - Multiple workers on same machine work perfectly

### When to Use

Perfect for:
- ✅ Single-node background job processing
- ✅ High-throughput local task scheduling
- ✅ Embedded systems without external dependencies

Not suitable for:
- ❌ Multi-server deployments
- ❌ Cloud-native microservices
- ❌ High-availability requirements

## Requirements

- Java 21+
- RocksDB 9.7.4+
- Jackson 2.18.2+ (for JSON)
- Kryo 5.6.2+ (for high-performance serialization)

## License

MIT License
