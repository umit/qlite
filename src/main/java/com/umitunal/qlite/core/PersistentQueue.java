package com.umitunal.qlite.core;

import java.util.Queue;

/**
 * Extension of Java's Queue interface with persistence and scheduling capabilities.
 *
 * This interface combines standard Queue operations with advanced features like:
 * - Persistent storage
 * - Scheduled/delayed execution
 * - Retry mechanisms
 * - Distributed worker coordination
 *
 * @param <E> the type of elements held in this queue
 */
public interface PersistentQueue<E> extends Queue<E>, AutoCloseable {

    /**
     * Schedule an element for future execution.
     *
     * @param element the element to add
     * @param scheduledTime when the element should be available (millis since epoch)
     * @return true if added successfully
     */
    boolean schedule(E element, long scheduledTime);

    /**
     * Retrieves and removes the head of this queue, blocking if necessary
     * until an element becomes available.
     *
     * @return the head of this queue
     * @throws InterruptedException if interrupted while waiting
     */
    E take() throws InterruptedException;

    /**
     * Retrieves but does not remove the next ready element.
     * An element is "ready" if its scheduled time has passed.
     *
     * @return the next ready element, or null if none ready
     */
    E peekReady();

    /**
     * Get the approximate size of the queue.
     * May not reflect the exact count in distributed scenarios.
     *
     * @return approximate number of elements
     */
    long approximateSize();

    /**
     * Get detailed metrics about the queue.
     *
     * @return queue metrics
     */
    QueueMetrics getMetrics() throws Exception;
}
