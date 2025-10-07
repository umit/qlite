package com.umitunal.qlite.model;

import com.umitunal.qlite.core.Job;
import com.umitunal.qlite.serialization.PayloadCodec;

import java.io.Serializable;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Concrete implementation of a Job with full state management.
 *
 * @param <T> the type of the job payload
 */
public class WorkUnit<T> implements Job<T>, Serializable {
    private final String id;
    private final T payload;
    private final long scheduledTime;
    private final int maxAttempts;

    private int currentAttempt;
    private long leaseExpiry;
    private String assignedWorker;
    private ExecutionState state;
    private long createdAt;
    private long lastModified;
    private String failureReason;
    private long version;  // For optimistic locking

    public WorkUnit(String id, T payload, long scheduledTime, int maxAttempts) {
        this.id = id;
        this.payload = payload;
        this.scheduledTime = scheduledTime;
        this.maxAttempts = maxAttempts;
        this.currentAttempt = 0;
        this.leaseExpiry = 0;
        this.state = ExecutionState.QUEUED;
        this.createdAt = System.currentTimeMillis();
        this.lastModified = this.createdAt;
        this.version = 0;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public T getPayload() {
        return payload;
    }

    @Override
    public long getScheduledTime() {
        return scheduledTime;
    }

    @Override
    public ExecutionState getState() {
        return state;
    }

    @Override
    public int getCurrentAttempt() {
        return currentAttempt;
    }

    @Override
    public int getMaxAttempts() {
        return maxAttempts;
    }

    @Override
    public boolean isReady() {
        return System.currentTimeMillis() >= scheduledTime;
    }

    @Override
    public boolean canRetry() {
        return currentAttempt < maxAttempts;
    }

    public long getLeaseExpiry() {
        return leaseExpiry;
    }

    public String getAssignedWorker() {
        return assignedWorker;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public long getLastModified() {
        return lastModified;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public long getVersion() {
        return version;
    }

    // Package-private setters for deserialization
    void setCurrentAttempt(int currentAttempt) {
        this.currentAttempt = currentAttempt;
    }

    void setLeaseExpiry(long leaseExpiry) {
        this.leaseExpiry = leaseExpiry;
    }

    void setAssignedWorker(String assignedWorker) {
        this.assignedWorker = assignedWorker;
    }

    void setState(ExecutionState state) {
        this.state = state;
    }

    void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    void setLastModified(long lastModified) {
        this.lastModified = lastModified;
    }

    void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    void setVersion(long version) {
        this.version = version;
    }

    public boolean isLeaseExpired() {
        return System.currentTimeMillis() > leaseExpiry;
    }

    public void lease(String workerId, long durationMs) {
        this.assignedWorker = workerId;
        this.leaseExpiry = System.currentTimeMillis() + durationMs;
        this.state = ExecutionState.LEASED;
        this.currentAttempt++;
        this.lastModified = System.currentTimeMillis();
        this.version++;  // Increment version on state change
    }

    public void extendLease(long additionalMs) {
        this.leaseExpiry += additionalMs;
        this.lastModified = System.currentTimeMillis();
        this.version++;
    }

    public void markSuccess() {
        this.state = ExecutionState.SUCCESS;
        this.lastModified = System.currentTimeMillis();
        this.version++;
    }

    public void markFailed(String reason) {
        this.failureReason = reason;
        this.state = ExecutionState.FAILED;
        this.lastModified = System.currentTimeMillis();
        this.version++;
    }

    public void markAbandoned() {
        this.state = ExecutionState.ABANDONED;
        this.lastModified = System.currentTimeMillis();
        this.version++;
    }

    public void resetForRetry() {
        this.state = ExecutionState.QUEUED;
        this.assignedWorker = null;
        this.leaseExpiry = 0;
        this.lastModified = System.currentTimeMillis();
        this.version++;
    }

    @Override
    public String toString() {
        return String.format("WorkUnit{id='%s', state=%s, attempt=%d/%d, scheduled=%d, worker='%s'}",
                id, state, currentAttempt, maxAttempts, scheduledTime, assignedWorker);
    }

    /**
     * Serialize to bytes for storage.
     * Delegates to WorkUnitSerializer for actual serialization logic.
     *
     * @param codec the payload codec to use
     * @return byte array representation
     */
    public byte[] serialize(PayloadCodec<T> codec) {
        return new WorkUnitSerializer<>(codec).serialize(this);
    }

    /**
     * Deserialize from bytes.
     * Delegates to WorkUnitSerializer for actual deserialization logic.
     *
     * @param bytes the byte array to deserialize
     * @param codec the payload codec to use
     * @return reconstructed WorkUnit
     */
    public static <T> WorkUnit<T> deserialize(byte[] bytes, PayloadCodec<T> codec) {
        return new WorkUnitSerializer<>(codec).deserialize(bytes);
    }

    /**
     * Create storage key for this work unit.
     * Format: [scheduledTime(8 bytes)][jobId bytes]
     * This ensures natural time-based ordering and prevents key collisions.
     */
    public static byte[] createStorageKey(long scheduledTime, String jobId) {
        byte[] jobIdBytes = jobId.getBytes(UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(8 + jobIdBytes.length);
        buffer.putLong(scheduledTime);
        buffer.put(jobIdBytes);
        return buffer.array();
    }
}
