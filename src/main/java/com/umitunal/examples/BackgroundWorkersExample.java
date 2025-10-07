package com.umitunal.examples;

import com.umitunal.qlite.config.StorageConfig;
import com.umitunal.qlite.core.JobQueue;
import com.umitunal.qlite.serialization.StringCodec;
import com.umitunal.qlite.storage.RocksJobQueue;
import com.umitunal.qlite.worker.JobProcessor;
import com.umitunal.qlite.worker.QueueWorker;

/**
 * Background workers example - demonstrates concurrent job processing.
 */
public class BackgroundWorkersExample {

    public static void main(String[] args) {
        System.out.println("=== Background Workers Example ===\n");

        StorageConfig config = StorageConfig.newBuilder("/tmp/qlite-workers")
                .build();

        try (JobQueue<String> queue = new RocksJobQueue<>(config, new StringCodec())) {

            // Submit batch of jobs
            long now = System.currentTimeMillis();
            for (int i = 1; i <= 12; i++) {
                queue.enqueue("job-" + i, "Task #" + i, now, 3);
            }

            System.out.println("Submitted 12 jobs");
            System.out.println(queue.getMetrics());

            // Create job processor
            JobProcessor<String> processor = job -> {
                System.out.println("  Worker processing: " + job.getId() + " - " + job.getPayload());
                Thread.sleep(500); // Simulate work
                return JobProcessor.ProcessingResult.success();
            };

            // Start 3 background workers
            QueueWorker<String> worker1 = QueueWorker.builder("worker-1", queue, processor)
                    .withLeaseDuration(2000)
                    .withPollInterval(500)
                    .build();

            QueueWorker<String> worker2 = QueueWorker.builder("worker-2", queue, processor)
                    .withLeaseDuration(2000)
                    .withPollInterval(500)
                    .build();

            QueueWorker<String> worker3 = QueueWorker.builder("worker-3", queue, processor)
                    .withLeaseDuration(2000)
                    .withPollInterval(500)
                    .build();

            worker1.start();
            worker2.start();
            worker3.start();

            // Wait for completion
            Thread.sleep(5000);

            worker1.stop();
            worker2.stop();
            worker3.stop();

            System.out.println("\nWorker 1 processed: " + worker1.getProcessedCount());
            System.out.println("Worker 2 processed: " + worker2.getProcessedCount());
            System.out.println("Worker 3 processed: " + worker3.getProcessedCount());

            System.out.println("\n" + queue.getMetrics());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
