package com.umitunal.examples;

import com.umitunal.qlite.config.StorageConfig;
import com.umitunal.qlite.core.PersistentQueue;
import com.umitunal.qlite.serialization.StringCodec;
import com.umitunal.qlite.storage.RocksPersistentQueue;

import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

/**
 * Example demonstrating standard Java Queue interface with persistence.
 */
public class StandardQueueExample {

    public static void main(String[] args) {
        System.out.println("=== Standard Java Queue Interface ===\n");

        // Example 1: Using as standard Queue
        standardQueueOperations();

        // Example 2: Scheduled/Delayed operations
        scheduledOperations();

        // Example 3: Blocking take operation
        blockingTakeExample();

        // Example 4: Collection operations
        collectionOperations();
    }

    private static void standardQueueOperations() {
        System.out.println("--- Example 1: Standard Queue Operations ---\n");

        StorageConfig config = StorageConfig.newBuilder("/tmp/std-queue-1")
                .withDurableWrites(false)
                .build();

        try (PersistentQueue<String> queue = new RocksPersistentQueue<>(
                config, new StringCodec(), "worker-1")) {

            // Standard Queue interface methods
            Queue<String> q = queue;  // Can treat as regular Queue

            // offer() - add elements
            q.offer("Task 1");
            q.offer("Task 2");
            q.offer("Task 3");

            System.out.println("Added 3 tasks");
            System.out.println("Size: " + q.size());
            System.out.println("Empty: " + q.isEmpty());

            // peek() - view without removing
            System.out.println("\nPeek: " + q.peek());

            // poll() - remove and return
            System.out.println("\nPolling:");
            while (!q.isEmpty()) {
                System.out.println("  " + q.poll());
            }

            System.out.println("\nEmpty: " + q.isEmpty());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void scheduledOperations() {
        System.out.println("\n--- Example 2: Scheduled Operations ---\n");

        StorageConfig config = StorageConfig.newBuilder("/tmp/std-queue-2")
                .build();

        try (PersistentQueue<String> queue = new RocksPersistentQueue<>(
                config, new StringCodec(), "worker-2")) {

            long now = System.currentTimeMillis();

            // Schedule tasks at different times
            queue.schedule("Execute now", now);
            queue.schedule("Execute in 2 seconds", now + 2000);
            queue.schedule("Execute in 5 seconds", now + 5000);

            System.out.println("Scheduled 3 tasks");
            System.out.println("Approximate size: " + queue.approximateSize());

            // Poll ready items
            System.out.println("\nAt t=0 (should get 1):");
            String task = queue.poll();
            if (task != null) {
                System.out.println("  " + task);
            }

            // Wait and poll again
            System.out.println("\nWaiting 3 seconds...");
            Thread.sleep(3000);

            System.out.println("At t=3s (should get 1):");
            task = queue.poll();
            if (task != null) {
                System.out.println("  " + task);
            }

            System.out.println("\nRemaining: " + queue.approximateSize());

            // Cleanup
            queue.clear();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void blockingTakeExample() {
        System.out.println("\n--- Example 3: Blocking Take ---\n");

        StorageConfig config = StorageConfig.newBuilder("/tmp/std-queue-3")
                .build();

        try (PersistentQueue<String> queue = new RocksPersistentQueue<>(
                config, new StringCodec(), "worker-3")) {

            // Start a consumer thread
            Thread consumer = new Thread(() -> {
                try {
                    System.out.println("Consumer waiting for tasks...");

                    for (int i = 0; i < 3; i++) {
                        String task = queue.take();  // Blocks until available
                        System.out.println("  Consumed: " + task);
                    }

                    System.out.println("Consumer done!");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            consumer.start();

            // Producer adds items with delay
            Thread.sleep(500);
            System.out.println("Producer: Adding task 1");
            queue.offer("Task 1");

            Thread.sleep(1000);
            System.out.println("Producer: Adding task 2");
            queue.offer("Task 2");

            Thread.sleep(1000);
            System.out.println("Producer: Adding task 3");
            queue.offer("Task 3");

            consumer.join(5000);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void collectionOperations() {
        System.out.println("\n--- Example 4: Collection Operations ---\n");

        StorageConfig config = StorageConfig.newBuilder("/tmp/std-queue-4")
                .build();

        try (PersistentQueue<String> queue = new RocksPersistentQueue<>(
                config, new StringCodec(), "worker-4")) {

            // addAll - batch add
            List<String> tasks = Arrays.asList("A", "B", "C", "D", "E");
            queue.addAll(tasks);

            System.out.println("Added batch: " + tasks);
            System.out.println("Size: " + queue.size());

            // contains
            System.out.println("\nContains 'C': " + queue.contains("C"));
            System.out.println("Contains 'Z': " + queue.contains("Z"));

            // remove specific element
            queue.remove("C");
            System.out.println("\nRemoved 'C'");
            System.out.println("Size: " + queue.size());

            // toArray
            System.out.println("\nQueue contents:");
            Object[] array = queue.toArray();
            for (Object item : array) {
                System.out.println("  " + item);
            }

            // clear
            queue.clear();
            System.out.println("\nCleared queue");
            System.out.println("Empty: " + queue.isEmpty());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
