package com.umitunal.qlite.core;

/**
 * Represents a unit of work to be executed by the queue system.
 *
 * @param <T> the type of the job payload
 */
public interface Job<T> {

    /**
     * Gets the unique identifier for this job.
     */
    String getId();

    /**
     * Gets the job payload data.
     */
    T getPayload();

    /**
     * Gets the scheduled execution time in milliseconds since epoch.
     */
    long getScheduledTime();

    /**
     * Gets the current execution state.
     */
    ExecutionState getState();

    /**
     * Gets the current attempt number (0-based).
     */
    int getCurrentAttempt();

    /**
     * Gets the maximum number of execution attempts allowed.
     */
    int getMaxAttempts();

    /**
     * Checks if this job is ready for execution.
     */
    boolean isReady();

    /**
     * Checks if this job can be retried.
     */
    boolean canRetry();

    /**
     * Possible execution states for a job.
     */
    enum ExecutionState {
        QUEUED,      // Waiting for execution
        LEASED,      // Leased by a worker
        SUCCESS,     // Successfully completed
        FAILED,      // Failed permanently
        ABANDONED    // Lease expired without completion
    }
}
