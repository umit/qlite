package com.umitunal.qlite.core;

import java.util.List;

/**
 * Core interface for a persistent job queue with scheduling and retry capabilities.
 *
 * @param <T> the type of job payload
 */
public interface JobQueue<T> extends AutoCloseable {

    /**
     * Enqueue a job for execution.
     *
     * @param jobId unique identifier for the job
     * @param payload the job data
     * @param scheduledTime when the job should be executed (millis since epoch)
     * @param maxAttempts maximum number of execution attempts
     */
    void enqueue(String jobId, T payload, long scheduledTime, int maxAttempts) throws Exception;

    /**
     * Acquire the next available job for processing.
     *
     * @param workerId unique identifier for the worker
     * @param leaseDuration how long the worker can hold the job (milliseconds)
     * @return the acquired job, or null if none available
     */
    Job<T> acquire(String workerId, long leaseDuration) throws Exception;

    /**
     * Acquire multiple jobs in a single operation.
     *
     * @param workerId unique identifier for the worker
     * @param limit maximum number of jobs to acquire
     * @param leaseDuration how long the worker can hold each job (milliseconds)
     * @return list of acquired jobs
     */
    List<Job<T>> acquireBatch(String workerId, int limit, long leaseDuration) throws Exception;

    /**
     * Mark a job as successfully completed.
     */
    void acknowledge(Job<T> job) throws Exception;

    /**
     * Mark a job as failed and potentially retry it.
     *
     * @param job the failed job
     * @param reason failure reason
     */
    void reject(Job<T> job, String reason) throws Exception;

    /**
     * Extend the lease time for a job that needs more processing time.
     */
    void renewLease(Job<T> job, long additionalTime) throws Exception;

    /**
     * Recover jobs with expired leases.
     *
     * @return number of jobs recovered
     */
    long recoverAbandoned() throws Exception;

    /**
     * Get statistics about the queue.
     */
    QueueMetrics getMetrics() throws Exception;
}
