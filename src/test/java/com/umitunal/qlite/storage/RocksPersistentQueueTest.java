package com.umitunal.qlite.storage;

import com.umitunal.qlite.config.StorageConfig;
import com.umitunal.qlite.core.PersistentQueue;
import com.umitunal.qlite.serialization.StringCodec;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.*;

class RocksPersistentQueueTest {

    @TempDir
    Path tempDir;

    private RocksPersistentQueue<String> queue;

    @BeforeEach
    void setUp() throws Exception {
        StorageConfig config = StorageConfig.newBuilder(tempDir.toString())
                .withDurableWrites(false)
                .build();
        queue = new RocksPersistentQueue<>(config, new StringCodec(), "test-worker");
    }

    @AfterEach
    void tearDown() {
        if (queue != null) {
            queue.close();
        }
    }

    // ========== Standard Queue Interface Tests ==========

    @Test
    @DisplayName("Should offer and poll elements")
    void testOfferAndPoll() {
        // When
        boolean offered = queue.offer("Task 1");

        // Then
        assertThat(offered).isTrue();
        assertThat(queue.poll()).isEqualTo("Task 1");
        assertThat(queue.poll()).isNull();
    }

    @Test
    @DisplayName("Should add and remove elements")
    void testAddAndRemove() {
        // When
        queue.add("Task 1");
        await().pollDelay(2, TimeUnit.MILLISECONDS).until(() -> true);
        queue.add("Task 2");

        // Then
        assertThat(queue.remove()).isEqualTo("Task 1");
        assertThat(queue.remove()).isEqualTo("Task 2");
    }

    @Test
    @DisplayName("Should throw exception on remove from empty queue")
    void testRemoveFromEmptyQueue() {
        assertThatThrownBy(() -> queue.remove())
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    @DisplayName("Should peek without removing")
    void testPeek() {
        // Given
        queue.offer("Task 1");
        await().pollDelay(2, TimeUnit.MILLISECONDS).until(() -> true);
        queue.offer("Task 2");

        // When/Then
        assertThat(queue.peek()).isEqualTo("Task 1");
        assertThat(queue.size()).isEqualTo(2); // Still has 2 items
    }

    @Test
    @DisplayName("Should element and throw if empty")
    void testElement() {
        // Given
        queue.offer("Task 1");

        // When/Then
        assertThat(queue.element()).isEqualTo("Task 1");

        queue.poll(); // Remove it
        assertThatThrownBy(() -> queue.element())
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    @DisplayName("Should report size correctly")
    void testSize() {
        // When
        queue.offer("Task 1");
        queue.offer("Task 2");
        queue.offer("Task 3");

        // Then
        assertThat(queue.size()).isEqualTo(3);
    }

    @Test
    @DisplayName("Should check if empty")
    void testIsEmpty() {
        // When/Then
        assertThat(queue.isEmpty()).isTrue();

        queue.offer("Task 1");
        assertThat(queue.isEmpty()).isFalse();

        queue.poll();
        assertThat(queue.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("Should check contains")
    void testContains() {
        // Given
        queue.offer("Task 1");
        queue.offer("Task 2");

        // When/Then
        assertThat(queue.contains("Task 1")).isTrue();
        assertThat(queue.contains("Task 3")).isFalse();
    }

    @Test
    @DisplayName("Should iterate over elements")
    void testIterator() {
        // Given
        queue.offer("Task 1");
        await().pollDelay(2, TimeUnit.MILLISECONDS).until(() -> true);
        queue.offer("Task 2");
        await().pollDelay(2, TimeUnit.MILLISECONDS).until(() -> true);
        queue.offer("Task 3");

        // When
        List<String> items = new java.util.ArrayList<>();
        queue.iterator().forEachRemaining(items::add);

        // Then
        assertThat(items).containsExactly("Task 1", "Task 2", "Task 3");
    }

    @Test
    @DisplayName("Should convert to array")
    void testToArray() {
        // Given
        queue.offer("Task 1");
        await().pollDelay(2, TimeUnit.MILLISECONDS).until(() -> true);
        queue.offer("Task 2");

        // When
        Object[] array = queue.toArray();

        // Then
        assertThat(array).containsExactly("Task 1", "Task 2");
    }

    @Test
    @DisplayName("Should remove specific element")
    void testRemoveSpecific() {
        // Given
        queue.offer("Task 1");
        queue.offer("Task 2");
        queue.offer("Task 3");

        // When
        boolean removed = queue.remove("Task 2");

        // Then
        assertThat(removed).isTrue();
        assertThat(queue.size()).isEqualTo(2);
        assertThat(queue.contains("Task 2")).isFalse();
    }

    @Test
    @DisplayName("Should add all from collection")
    void testAddAll() {
        // Given
        List<String> tasks = Arrays.asList("Task 1", "Task 2", "Task 3");

        // When
        boolean modified = queue.addAll(tasks);

        // Then
        assertThat(modified).isTrue();
        assertThat(queue.size()).isEqualTo(3);
    }

    @Test
    @DisplayName("Should clear all elements")
    void testClear() {
        // Given
        queue.offer("Task 1");
        queue.offer("Task 2");

        // When
        queue.clear();

        // Then
        assertThat(queue.isEmpty()).isTrue();
        assertThat(queue.size()).isEqualTo(0);
    }

    // ========== PersistentQueue Extended Interface Tests ==========

    @Test
    @DisplayName("Should schedule tasks for future execution")
    void testSchedule() {
        // Given
        long now = System.currentTimeMillis();
        long futureTime = now + 5000;

        // When
        queue.schedule("Future task", futureTime);
        queue.schedule("Current task", now);

        // Then - Only current task should be available
        assertThat(queue.poll()).isEqualTo("Current task");
        assertThat(queue.poll()).isNull(); // Future task not ready
    }

    @Test
    @DisplayName("Should peek only ready tasks")
    void testPeekReady() {
        // Given
        long now = System.currentTimeMillis();
        queue.schedule("Ready task", now);
        queue.schedule("Future task", now + 10000);

        // When/Then
        assertThat(queue.peekReady()).isEqualTo("Ready task");
    }

    @Test
    @Order(16)
    @DisplayName("Should block on take until element available")
    void testTake() throws Exception {
        // Given
        AtomicReference<String> result = new AtomicReference<>();

        Thread consumer = new Thread(() -> {
            try {
                String task = queue.take();  // Blocks
                result.set(task);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // When
        consumer.start();

        // Producer adds item after delay
        await().pollDelay(500, TimeUnit.MILLISECONDS)
               .untilAsserted(() -> queue.offer("Delayed task"));

        // Then - Wait for consumer to process
        await().atMost(2, TimeUnit.SECONDS)
               .until(() -> result.get() != null);

        assertThat(result.get()).isEqualTo("Delayed task");
        consumer.join();
    }

    @Test
    @Order(17)
    @DisplayName("Should get approximate size")
    void testApproximateSize() {
        // Given
        for (int i = 0; i < 10; i++) {
            queue.offer("Task " + i);
        }

        // When
        long size = queue.approximateSize();

        // Then
        assertThat(size).isGreaterThanOrEqualTo(10);
    }

    @Test
    @Order(18)
    @DisplayName("Should work with Queue interface")
    void testQueueInterface() {
        // Given - Use as standard Queue
        Queue<String> q = queue;

        // When
        q.offer("Task 1");
        await().pollDelay(2, TimeUnit.MILLISECONDS).until(() -> true);
        q.offer("Task 2");

        // Then
        assertThat(q.poll()).isEqualTo("Task 1");
        assertThat(q.peek()).isEqualTo("Task 2");
        assertThat(q.size()).isEqualTo(1);
    }

    @Test
    @Order(19)
    @DisplayName("Should handle containsAll")
    void testContainsAll() {
        // Given
        queue.addAll(Arrays.asList("A", "B", "C"));

        // When/Then
        assertThat(queue.containsAll(Arrays.asList("A", "B"))).isTrue();
        assertThat(queue.containsAll(Arrays.asList("A", "D"))).isFalse();
    }

    @Test
    @Order(20)
    @DisplayName("Should handle removeAll")
    void testRemoveAll() {
        // Given
        queue.addAll(Arrays.asList("A", "B", "C", "D"));

        // When
        boolean modified = queue.removeAll(Arrays.asList("B", "D"));

        // Then
        assertThat(modified).isTrue();
        assertThat(queue.size()).isEqualTo(2);
        assertThat(queue.contains("B")).isFalse();
        assertThat(queue.contains("D")).isFalse();
    }
}
