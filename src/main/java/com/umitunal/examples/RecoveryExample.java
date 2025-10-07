package com.umitunal.examples;

import com.umitunal.qlite.config.StorageConfig;
import com.umitunal.qlite.core.Job;
import com.umitunal.qlite.serialization.StringCodec;
import com.umitunal.qlite.storage.RocksJobQueue;

/**
 * Job recovery example - demonstrates recovering abandoned jobs.
 */
public class RecoveryExample {

    public static void main(String[] args) {
        System.out.println("=== Job Recovery Example ===\n");

        StorageConfig config = StorageConfig.newBuilder("/tmp/qlite-recovery")
                .build();

        try (RocksJobQueue<String> queue = new RocksJobQueue<>(config, new StringCodec(), 1000)) {

            // Submit jobs
            long now = System.currentTimeMillis();
            queue.enqueue("abandoned-1", "Job that will be abandoned", now, 3);
            queue.enqueue("abandoned-2", "Another abandoned job", now, 3);

            System.out.println("Submitted 2 jobs");

            // Acquire but don't complete (simulating crash)
            String workerId = "worker-crash";
            Job<String> job1 = queue.acquire(workerId, 1000);
            Job<String> job2 = queue.acquire(workerId, 1000);

            System.out.println("\nWorker acquired jobs but crashed:");
            System.out.println("  " + job1);
            System.out.println("  " + job2);
            System.out.println("\n" + queue.getMetrics());

            // Wait for leases to expire
            System.out.println("\nWaiting for leases to expire (1.5s)...");
            Thread.sleep(1500);

            // Recover abandoned jobs
            long recovered = queue.recoverAbandoned();
            System.out.println("Recovered " + recovered + " abandoned jobs");
            System.out.println(queue.getMetrics());

            // New worker can now process them
            String newWorkerId = "worker-recovery";
            System.out.println("\nNew worker processing recovered jobs:");

            Job<String> job;
            while ((job = queue.acquire(newWorkerId, 5000)) != null) {
                System.out.println("  " + job);
                queue.acknowledge(job);
            }

            System.out.println("\n" + queue.getMetrics());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
