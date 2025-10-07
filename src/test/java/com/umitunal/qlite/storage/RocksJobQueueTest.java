package com.umitunal.qlite.storage;

import com.umitunal.qlite.config.StorageConfig;
import com.umitunal.qlite.core.Job;
import com.umitunal.qlite.core.QueueMetrics;
import com.umitunal.qlite.serialization.StringCodec;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RocksJobQueueTest {

    @TempDir
    Path tempDir;

    private RocksJobQueue<String> queue;
    private StorageConfig config;

    @BeforeEach
    void setUp() throws Exception {
        config = StorageConfig.newBuilder(tempDir.toString())
                .withDurableWrites(false)
                .build();
        queue = new RocksJobQueue<>(config, new StringCodec());
    }

    @AfterEach
    void tearDown() {
        if (queue != null) {
            queue.close();
        }
    }

    @Test
    @Order(1)
    @DisplayName("Should enqueue and acquire job successfully")
    void testBasicEnqueueAndAcquire() throws Exception {
        // Given
        String jobId = "job-1";
        String payload = "Test payload";
        long now = System.currentTimeMillis();

        // When
        queue.enqueue(jobId, payload, now, 3);

        // Then
        Job<String> acquired = queue.acquire("worker-1");
        assertThat(acquired).isNotNull();
        assertThat(acquired.getId()).isEqualTo(jobId);
        assertThat(acquired.getPayload()).isEqualTo(payload);
        assertThat(acquired.getState()).isEqualTo(Job.ExecutionState.LEASED);
        assertThat(acquired.getCurrentAttempt()).isEqualTo(1);
    }

    @Test
    @Order(2)
    @DisplayName("Should not acquire future scheduled jobs")
    void testScheduledJobNotAcquired() throws Exception {
        // Given
        String jobId = "future-job";
        long futureTime = System.currentTimeMillis() + 10000; // 10 seconds in future

        // When
        queue.enqueue(jobId, "Future task", futureTime, 3);

        // Then
        Job<String> acquired = queue.acquire("worker-1");
        assertThat(acquired).isNull();
    }

    @Test
    @Order(3)
    @DisplayName("Should acknowledge and remove completed job")
    void testAcknowledge() throws Exception {
        // Given
        queue.enqueue("job-1", "Task 1", System.currentTimeMillis(), 3);
        Job<String> job = queue.acquire("worker-1");

        // When
        queue.acknowledge(job);

        // Then
        QueueMetrics metrics = queue.getMetrics();
        assertThat(metrics.getTotalJobs()).isEqualTo(0);
    }

    @Test
    @Order(4)
    @DisplayName("Should retry failed job")
    void testRetryMechanism() throws Exception {
        // Given
        String jobId = "retry-job";
        queue.enqueue(jobId, "Retry task", System.currentTimeMillis(), 3);

        // When - First attempt fails
        Job<String> job1 = queue.acquire("worker-1");
        queue.reject(job1, "Network error");

        // Then - Can acquire again
        Job<String> job2 = queue.acquire("worker-1");
        assertThat(job2).isNotNull();
        assertThat(job2.getId()).isEqualTo(jobId);
        assertThat(job2.getCurrentAttempt()).isEqualTo(2);
    }

    @Test
    @Order(5)
    @DisplayName("Should mark as failed after max attempts")
    void testMaxAttemptsExceeded() throws Exception {
        // Given
        queue.enqueue("fail-job", "Task", System.currentTimeMillis(), 2);

        // When - Fail twice
        Job<String> job1 = queue.acquire("worker-1");
        queue.reject(job1, "Error 1");

        Job<String> job2 = queue.acquire("worker-1");
        queue.reject(job2, "Error 2");

        // Then - No more attempts
        Job<String> job3 = queue.acquire("worker-1");
        assertThat(job3).isNull();

        QueueMetrics metrics = queue.getMetrics();
        assertThat(metrics.getFailedJobs()).isEqualTo(1);
    }

    @Test
    @Order(6)
    @DisplayName("Should extend lease for long-running jobs")
    void testLeaseExtension() throws Exception {
        // Given
        queue.enqueue("long-job", "Long task", System.currentTimeMillis(), 3);
        Job<String> job = queue.acquire("worker-1", 1000); // 1 second lease

        // When
        queue.renewLease(job, 5000); // Add 5 more seconds

        // Then - Lease should be extended (no automatic test for this, but no exception)
        assertThatCode(() -> queue.renewLease(job, 1000)).doesNotThrowAnyException();
    }

    @Test
    @Order(7)
    @DisplayName("Should recover abandoned jobs")
    void testAbandonedJobRecovery() throws Exception {
        // Given - Create job with short lease
        queue.enqueue("abandoned-job", "Task", System.currentTimeMillis(), 3);
        Job<String> job = queue.acquire("worker-1", 100); // 100ms lease

        // When - Wait for lease to expire using Awaitility
        await().atMost(500, TimeUnit.MILLISECONDS)
               .pollInterval(50, TimeUnit.MILLISECONDS)
               .until(() -> {
                   long recovered = queue.recoverAbandoned();
                   return recovered == 1;
               });

        // Then
        QueueMetrics metrics = queue.getMetrics();
        assertThat(metrics.getAbandonedJobs()).isEqualTo(1);
    }

    @Test
    @Order(8)
    @DisplayName("Should handle batch acquire")
    void testBatchAcquire() throws Exception {
        // Given
        for (int i = 1; i <= 5; i++) {
            queue.enqueue("job-" + i, "Task " + i, System.currentTimeMillis(), 3);
        }

        // When
        List<Job<String>> jobs = queue.acquireBatch("worker-1", 3, 5000);

        // Then
        assertThat(jobs).hasSize(3);
        assertThat(jobs).extracting(Job::getPayload)
                .containsExactly("Task 1", "Task 2", "Task 3");
    }

    @Test
    @Order(9)
    @DisplayName("Should get accurate metrics")
    void testMetrics() throws Exception {
        // Given
        long now = System.currentTimeMillis();
        queue.enqueue("queued-1", "Task 1", now, 3);
        queue.enqueue("queued-2", "Task 2", now, 3);

        Job<String> leased = queue.acquire("worker-1");

        queue.enqueue("failed-job", "Task", now, 1);
        Job<String> failJob = queue.acquire("worker-1");
        queue.reject(failJob, "Error");

        // When
        QueueMetrics metrics = queue.getMetrics();

        // Then
        assertThat(metrics.getTotalJobs()).isEqualTo(3);
        assertThat(metrics.getQueuedJobs()).isEqualTo(1);
        assertThat(metrics.getLeasedJobs()).isEqualTo(1);
        assertThat(metrics.getFailedJobs()).isEqualTo(1);
    }

    @Test
    @Order(10)
    @DisplayName("Should prevent concurrent worker race conditions with versioning")
    void testConcurrentWorkers() throws Exception {
        // Given
        queue.enqueue("concurrent-job", "Task", System.currentTimeMillis(), 3);

        // When - Two workers try to acquire simultaneously
        Job<String> job1 = queue.acquire("worker-1");
        Job<String> job2 = queue.acquire("worker-2");

        // Then - Only one should succeed
        assertThat(job1).isNotNull();
        assertThat(job2).isNull(); // Second worker shouldn't get the same job
    }

    @Test
    @Order(11)
    @DisplayName("Should throw exception when rejecting non-existent job")
    void testRejectNonExistentJob() throws Exception {
        // Given
        queue.enqueue("job-1", "Task", System.currentTimeMillis(), 3);
        Job<String> job = queue.acquire("worker-1");
        queue.acknowledge(job); // Remove from queue

        // When/Then
        assertThatThrownBy(() -> queue.reject(job, "Error"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Job not found");
    }

    @Test
    @Order(12)
    @DisplayName("Should purge failed jobs")
    void testPurgeFailedJobs() throws Exception {
        // Given
        queue.enqueue("fail-1", "Task 1", System.currentTimeMillis(), 1);
        queue.enqueue("fail-2", "Task 2", System.currentTimeMillis(), 1);

        Job<String> job1 = queue.acquire("worker-1");
        queue.reject(job1, "Error");

        Job<String> job2 = queue.acquire("worker-1");
        queue.reject(job2, "Error");

        // When
        long purged = queue.purgeFailedJobs();

        // Then
        assertThat(purged).isEqualTo(2);

        QueueMetrics metrics = queue.getMetrics();
        assertThat(metrics.getTotalJobs()).isEqualTo(0);
    }

    @Test
    @Order(13)
    @DisplayName("Should handle unique job IDs correctly")
    void testUniqueJobIds() throws Exception {
        // Given
        String jobId = "unique-job";
        long now = System.currentTimeMillis();

        // When - Same job ID, different scheduled times
        queue.enqueue(jobId, "First", now, 3);
        queue.enqueue(jobId, "Second", now + 1000, 3);

        // Then - Should have 2 separate jobs (different keys due to different times)
        QueueMetrics metrics = queue.getMetrics();
        assertThat(metrics.getTotalJobs()).isEqualTo(2);
    }

    @Test
    @Order(14)
    @DisplayName("Should use default lease duration")
    void testDefaultLeaseDuration() throws Exception {
        // Given
        queue.enqueue("job-1", "Task", System.currentTimeMillis(), 3);

        // When - Use convenience method without lease duration
        Job<String> job = queue.acquire("worker-1");

        // Then
        assertThat(job).isNotNull();
        assertThat(job.getState()).isEqualTo(Job.ExecutionState.LEASED);
    }
}
