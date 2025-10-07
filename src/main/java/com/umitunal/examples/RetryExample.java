package com.umitunal.examples;

import com.umitunal.qlite.config.StorageConfig;
import com.umitunal.qlite.core.Job;
import com.umitunal.qlite.core.JobQueue;
import com.umitunal.qlite.serialization.StringCodec;
import com.umitunal.qlite.storage.RocksJobQueue;

/**
 * Retry mechanism example - demonstrates automatic retry on failures.
 */
public class RetryExample {

    public static void main(String[] args) {
        System.out.println("=== Retry Mechanism Example ===\n");

        StorageConfig config = StorageConfig.newBuilder("/tmp/qlite-retry")
                .build();

        try (JobQueue<String> queue = new RocksJobQueue<>(config, new StringCodec())) {

            // Submit a job with max 3 attempts
            long now = System.currentTimeMillis();
            queue.enqueue("flaky-job", "Unreliable API call", now, 3);

            System.out.println("Submitted job with max 3 attempts");
            String workerId = "worker-retry";

            // Simulate failures and eventual success
            for (int attempt = 1; attempt <= 4; attempt++) {
                Job<String> job = queue.acquire(workerId, 5000);

                if (job == null) {
                    System.out.println("\nNo more jobs (failed permanently)");
                    break;
                }

                System.out.println("\nAttempt " + attempt + ": " + job);

                if (attempt < 3) {
                    System.out.println("  Failed - will retry");
                    queue.reject(job, "Connection timeout");
                } else {
                    System.out.println("  Success!");
                    queue.acknowledge(job);
                }

                Thread.sleep(100);
            }

            System.out.println("\n" + queue.getMetrics());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
