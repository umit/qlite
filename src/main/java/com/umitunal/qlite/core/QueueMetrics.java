package com.umitunal.qlite.core;

/**
 * Metrics and statistics for queue monitoring.
 */
public class QueueMetrics {
    private final long totalJobs;
    private final long queuedJobs;
    private final long leasedJobs;
    private final long successfulJobs;
    private final long failedJobs;
    private final long abandonedJobs;

    public QueueMetrics(long totalJobs, long queuedJobs, long leasedJobs,
                       long successfulJobs, long failedJobs, long abandonedJobs) {
        this.totalJobs = totalJobs;
        this.queuedJobs = queuedJobs;
        this.leasedJobs = leasedJobs;
        this.successfulJobs = successfulJobs;
        this.failedJobs = failedJobs;
        this.abandonedJobs = abandonedJobs;
    }

    public long getTotalJobs() { return totalJobs; }
    public long getQueuedJobs() { return queuedJobs; }
    public long getLeasedJobs() { return leasedJobs; }
    public long getSuccessfulJobs() { return successfulJobs; }
    public long getFailedJobs() { return failedJobs; }
    public long getAbandonedJobs() { return abandonedJobs; }

    @Override
    public String toString() {
        return String.format(
            "QueueMetrics{total=%d, queued=%d, leased=%d, success=%d, failed=%d, abandoned=%d}",
            totalJobs, queuedJobs, leasedJobs, successfulJobs, failedJobs, abandonedJobs
        );
    }
}
