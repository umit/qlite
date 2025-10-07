package com.umitunal.examples;

import com.umitunal.qlite.config.StorageConfig;
import com.umitunal.qlite.core.Job;
import com.umitunal.qlite.core.JobQueue;
import com.umitunal.qlite.serialization.StringCodec;
import com.umitunal.qlite.storage.RocksJobQueue;

/**
 * Time-based scheduling example - demonstrates delayed job execution.
 */
public class ScheduledJobsExample {

    public static void main(String[] args) {
        System.out.println("=== Scheduled Jobs Example ===\n");

        StorageConfig config = StorageConfig.newBuilder("/tmp/qlite-scheduled")
                .build();

        try (JobQueue<String> queue = new RocksJobQueue<>(config, new StringCodec())) {

            long now = System.currentTimeMillis();

            // Schedule jobs at different times
            queue.enqueue("immediate", "Run now", now, 3);
            queue.enqueue("delayed-2s", "Run in 2 seconds", now + 2000, 3);
            queue.enqueue("delayed-5s", "Run in 5 seconds", now + 5000, 3);
            queue.enqueue("delayed-8s", "Run in 8 seconds", now + 8000, 3);

            System.out.println("Scheduled 4 jobs");
            System.out.println(queue.getMetrics());

            String workerId = "worker-scheduler";

            // Process immediately available
            System.out.println("\nAt t=0:");
            Job<String> job = queue.acquire(workerId, 5000);
            if (job != null) {
                System.out.println("  Acquired: " + job.getPayload());
                queue.acknowledge(job);
            }

            // Wait and process more
            System.out.println("\nWaiting 3 seconds...");
            Thread.sleep(3000);

            System.out.println("At t=3s:");
            job = queue.acquire(workerId, 5000);
            if (job != null) {
                System.out.println("  Acquired: " + job.getPayload());
                queue.acknowledge(job);
            }

            System.out.println("\n" + queue.getMetrics());

            // Cleanup
            while ((job = queue.acquire(workerId, 5000)) != null) {
                queue.acknowledge(job);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
