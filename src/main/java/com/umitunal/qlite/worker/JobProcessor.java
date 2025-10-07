package com.umitunal.qlite.worker;

import com.umitunal.qlite.core.Job;

/**
 * Interface for processing jobs acquired from the queue.
 *
 * @param <T> the type of job payload
 */
@FunctionalInterface
public interface JobProcessor<T> {

    /**
     * Process a job and return the result.
     *
     * @param job the job to process
     * @return processing result
     * @throws Exception if processing fails
     */
    ProcessingResult process(Job<T> job) throws Exception;

    /**
     * Result of job processing.
     */
    class ProcessingResult {
        private final boolean success;
        private final String message;

        private ProcessingResult(boolean success, String message) {
            this.success = success;
            this.message = message;
        }

        public boolean isSuccess() { return success; }
        public String getMessage() { return message; }

        public static ProcessingResult success() {
            return new ProcessingResult(true, null);
        }

        public static ProcessingResult success(String message) {
            return new ProcessingResult(true, message);
        }

        public static ProcessingResult failure(String message) {
            return new ProcessingResult(false, message);
        }
    }
}
