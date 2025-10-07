package com.umitunal.qlite.worker;

import com.umitunal.qlite.core.Job;
import com.umitunal.qlite.core.JobQueue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A worker that continuously polls the queue and processes jobs.
 *
 * @param <T> the type of job payload
 */
public class QueueWorker<T> implements AutoCloseable {
    private final String workerId;
    private final JobQueue<T> queue;
    private final JobProcessor<T> processor;
    private final long leaseDuration;
    private final long pollInterval;
    private final AtomicBoolean running;
    private final AtomicLong processedCount;
    private final AtomicLong failedCount;

    private Thread workerThread;

    private QueueWorker(Builder<T> builder) {
        this.workerId = builder.workerId;
        this.queue = builder.queue;
        this.processor = builder.processor;
        this.leaseDuration = builder.leaseDuration;
        this.pollInterval = builder.pollInterval;
        this.running = new AtomicBoolean(false);
        this.processedCount = new AtomicLong(0);
        this.failedCount = new AtomicLong(0);
    }

    /**
     * Start the worker in the background.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            workerThread = new Thread(this::run, "QueueWorker-" + workerId);
            workerThread.setDaemon(false);
            workerThread.start();
        }
    }

    /**
     * Stop the worker gracefully.
     */
    public void stop() {
        running.set(false);
        if (workerThread != null) {
            try {
                workerThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Process a single job synchronously.
     */
    public boolean processOne() throws Exception {
        Job<T> job = queue.acquire(workerId, leaseDuration);

        if (job == null) {
            return false;
        }

        try {
            JobProcessor.ProcessingResult result = processor.process(job);

            if (result.isSuccess()) {
                queue.acknowledge(job);
                processedCount.incrementAndGet();
                return true;
            } else {
                queue.reject(job, result.getMessage());
                failedCount.incrementAndGet();
                return false;
            }
        } catch (Exception e) {
            queue.reject(job, e.getMessage());
            failedCount.incrementAndGet();
            throw e;
        }
    }

    private void run() {
        while (running.get()) {
            try {
                boolean processed = processOne();

                if (!processed) {
                    // No jobs available, wait before polling again
                    Thread.sleep(pollInterval);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // Log error and continue
                System.err.println("Worker " + workerId + " error: " + e.getMessage());
            }
        }
    }

    public String getWorkerId() { return workerId; }
    public long getProcessedCount() { return processedCount.get(); }
    public long getFailedCount() { return failedCount.get(); }
    public boolean isRunning() { return running.get(); }

    @Override
    public void close() {
        stop();
    }

    public static <T> Builder<T> builder(String workerId, JobQueue<T> queue, JobProcessor<T> processor) {
        return new Builder<>(workerId, queue, processor);
    }

    public static class Builder<T> {
        private final String workerId;
        private final JobQueue<T> queue;
        private final JobProcessor<T> processor;
        private long leaseDuration = 30000; // 30 seconds
        private long pollInterval = 1000;   // 1 second

        private Builder(String workerId, JobQueue<T> queue, JobProcessor<T> processor) {
            this.workerId = workerId;
            this.queue = queue;
            this.processor = processor;
        }

        public Builder<T> withLeaseDuration(long millis) {
            this.leaseDuration = millis;
            return this;
        }

        public Builder<T> withPollInterval(long millis) {
            this.pollInterval = millis;
            return this;
        }

        public QueueWorker<T> build() {
            return new QueueWorker<>(this);
        }
    }
}
