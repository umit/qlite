package com.umitunal.qlite.model;

import com.umitunal.qlite.core.Job;
import com.umitunal.qlite.serialization.PayloadCodec;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * High-performance serializer for WorkUnit using ByteBuffer.
 *
 * Binary format:
 * - id length (4 bytes) + id bytes (UTF-8)
 * - payload length (4 bytes) + payload bytes
 * - scheduledTime (8 bytes)
 * - maxAttempts (4 bytes)
 * - currentAttempt (4 bytes)
 * - leaseExpiry (8 bytes)
 * - assignedWorker length (4 bytes) + worker bytes (UTF-8)
 * - state ordinal (4 bytes)
 * - createdAt (8 bytes)
 * - lastModified (8 bytes)
 * - failureReason length (4 bytes) + reason bytes (UTF-8)
 * - version (8 bytes)
 *
 * @param <T> the type of job payload
 */
public class WorkUnitSerializer<T> {

    private final PayloadCodec<T> payloadCodec;

    public WorkUnitSerializer(PayloadCodec<T> payloadCodec) {
        this.payloadCodec = payloadCodec;
    }

    /**
     * Serialize WorkUnit to bytes for storage.
     *
     * @param unit the work unit to serialize
     * @return byte array representation
     */
    public byte[] serialize(WorkUnit<T> unit) {
        byte[] idBytes = unit.getId().getBytes(UTF_8);
        byte[] payloadBytes = payloadCodec.encode(unit.getPayload());
        byte[] workerBytes = unit.getAssignedWorker() != null
            ? unit.getAssignedWorker().getBytes(UTF_8)
            : new byte[0];
        byte[] reasonBytes = unit.getFailureReason() != null
            ? unit.getFailureReason().getBytes(UTF_8)
            : new byte[0];

        // Calculate total size
        int totalSize = 4 + idBytes.length +           // id
                       4 + payloadBytes.length +        // payload
                       8 +                              // scheduledTime
                       4 +                              // maxAttempts
                       4 +                              // currentAttempt
                       8 +                              // leaseExpiry
                       4 + workerBytes.length +         // assignedWorker
                       4 +                              // state ordinal
                       8 +                              // createdAt
                       8 +                              // lastModified
                       4 + reasonBytes.length +         // failureReason
                       8;                               // version

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        // Write id
        buffer.putInt(idBytes.length);
        buffer.put(idBytes);

        // Write payload
        buffer.putInt(payloadBytes.length);
        buffer.put(payloadBytes);

        // Write metadata
        buffer.putLong(unit.getScheduledTime());
        buffer.putInt(unit.getMaxAttempts());
        buffer.putInt(unit.getCurrentAttempt());
        buffer.putLong(unit.getLeaseExpiry());

        // Write assignedWorker
        buffer.putInt(workerBytes.length);
        buffer.put(workerBytes);

        // Write state as ordinal
        buffer.putInt(unit.getState().ordinal());

        // Write timestamps
        buffer.putLong(unit.getCreatedAt());
        buffer.putLong(unit.getLastModified());

        // Write failureReason
        buffer.putInt(reasonBytes.length);
        buffer.put(reasonBytes);

        // Write version
        buffer.putLong(unit.getVersion());

        return buffer.array();
    }

    /**
     * Deserialize bytes to WorkUnit.
     *
     * @param bytes the byte array to deserialize
     * @return reconstructed WorkUnit
     */
    public WorkUnit<T> deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        // Read id
        int idLength = buffer.getInt();
        byte[] idBytes = new byte[idLength];
        buffer.get(idBytes);
        String id = new String(idBytes, UTF_8);

        // Read payload
        int payloadLength = buffer.getInt();
        byte[] payloadBytes = new byte[payloadLength];
        buffer.get(payloadBytes);
        T payload = payloadCodec.decode(payloadBytes);

        // Read metadata
        long scheduledTime = buffer.getLong();
        int maxAttempts = buffer.getInt();

        // Create WorkUnit with builder/constructor
        WorkUnit<T> unit = new WorkUnit<>(id, payload, scheduledTime, maxAttempts);

        // Restore internal state
        unit.setCurrentAttempt(buffer.getInt());
        unit.setLeaseExpiry(buffer.getLong());

        // Read assignedWorker
        int workerLength = buffer.getInt();
        if (workerLength > 0) {
            byte[] workerBytes = new byte[workerLength];
            buffer.get(workerBytes);
            unit.setAssignedWorker(new String(workerBytes, UTF_8));
        }

        // Read state (ordinal)
        unit.setState(Job.ExecutionState.values()[buffer.getInt()]);

        // Read timestamps
        unit.setCreatedAt(buffer.getLong());
        unit.setLastModified(buffer.getLong());

        // Read failureReason
        int reasonLength = buffer.getInt();
        if (reasonLength > 0) {
            byte[] reasonBytes = new byte[reasonLength];
            buffer.get(reasonBytes);
            unit.setFailureReason(new String(reasonBytes, UTF_8));
        }

        // Read version
        unit.setVersion(buffer.getLong());

        return unit;
    }

    /**
     * Calculate the size that would be needed to serialize this unit.
     * Useful for pre-allocation or size estimates.
     *
     * @param unit the work unit
     * @return estimated byte size
     */
    public int estimateSize(WorkUnit<T> unit) {
        int idSize = unit.getId().getBytes(UTF_8).length;
        int payloadSize = payloadCodec.encode(unit.getPayload()).length;
        int workerSize = unit.getAssignedWorker() != null
            ? unit.getAssignedWorker().getBytes(UTF_8).length
            : 0;
        int reasonSize = unit.getFailureReason() != null
            ? unit.getFailureReason().getBytes(UTF_8).length
            : 0;

        return 4 + idSize +           // id
               4 + payloadSize +      // payload
               8 +                    // scheduledTime
               4 +                    // maxAttempts
               4 +                    // currentAttempt
               8 +                    // leaseExpiry
               4 + workerSize +       // assignedWorker
               4 +                    // state ordinal
               8 +                    // createdAt
               8 +                    // lastModified
               4 + reasonSize +       // failureReason
               8;                     // version
    }
}
