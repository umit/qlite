package com.umitunal.examples;

import com.umitunal.qlite.config.StorageConfig;
import com.umitunal.qlite.core.Job;
import com.umitunal.qlite.core.JobQueue;
import com.umitunal.qlite.serialization.StringCodec;
import com.umitunal.qlite.storage.RocksJobQueue;

/**
 * Basic job processing example.
 */
public class BasicExample {

    public static void main(String[] args) {
        System.out.println("=== Basic Job Processing Example ===\n");

        StorageConfig config = StorageConfig.newBuilder("/tmp/qlite-basic")
                .withDurableWrites(false)
                .build();

        try (JobQueue<String> queue = new RocksJobQueue<>(config, new StringCodec())) {

            // Enqueue jobs
            long now = System.currentTimeMillis();
            queue.enqueue("job-1", "Send notification", now, 3);
            queue.enqueue("job-2", "Update database", now, 3);
            queue.enqueue("job-3", "Generate thumbnail", now, 3);

            System.out.println("Enqueued 3 jobs");
            System.out.println(queue.getMetrics());

            // Process jobs manually
            String workerId = "worker-main";
            System.out.println("\nProcessing jobs:");

            Job<String> job;
            while ((job = queue.acquire(workerId, 5000)) != null) {
                System.out.println("  Processing: " + job);

                // Simulate work
                Thread.sleep(100);

                queue.acknowledge(job);
                System.out.println("  Completed: " + job.getId());
            }

            System.out.println("\n" + queue.getMetrics());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
