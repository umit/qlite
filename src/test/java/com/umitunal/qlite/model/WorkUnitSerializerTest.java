package com.umitunal.qlite.model;

import com.umitunal.qlite.core.Job;
import com.umitunal.qlite.serialization.JsonCodec;
import com.umitunal.qlite.serialization.KryoCodec;
import com.umitunal.qlite.serialization.StringCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class WorkUnitSerializerTest {

    @Test
    @DisplayName("Should serialize and deserialize simple WorkUnit")
    void testSimpleWorkUnit() {
        // Given
        WorkUnitSerializer<String> serializer = new WorkUnitSerializer<>(new StringCodec());
        WorkUnit<String> original = new WorkUnit<>("job-1", "Task payload", System.currentTimeMillis(), 3);

        // When
        byte[] serialized = serializer.serialize(original);
        WorkUnit<String> deserialized = serializer.deserialize(serialized);

        // Then
        assertThat(deserialized.getId()).isEqualTo(original.getId());
        assertThat(deserialized.getPayload()).isEqualTo(original.getPayload());
        assertThat(deserialized.getScheduledTime()).isEqualTo(original.getScheduledTime());
        assertThat(deserialized.getMaxAttempts()).isEqualTo(original.getMaxAttempts());
        assertThat(deserialized.getCurrentAttempt()).isEqualTo(original.getCurrentAttempt());
        assertThat(deserialized.getState()).isEqualTo(original.getState());
    }

    @Test
    @DisplayName("Should preserve all metadata fields")
    void testMetadataPreservation() {
        // Given
        WorkUnitSerializer<String> serializer = new WorkUnitSerializer<>(new StringCodec());
        long now = System.currentTimeMillis();
        WorkUnit<String> original = new WorkUnit<>("job-2", "Complex task", now, 5);

        // Modify state
        original.lease("worker-1", 30000);
        original.markFailed("Connection timeout");

        // When
        byte[] serialized = serializer.serialize(original);
        WorkUnit<String> deserialized = serializer.deserialize(serialized);

        // Then
        assertThat(deserialized.getId()).isEqualTo("job-2");
        assertThat(deserialized.getPayload()).isEqualTo("Complex task");
        assertThat(deserialized.getScheduledTime()).isEqualTo(now);
        assertThat(deserialized.getMaxAttempts()).isEqualTo(5);
        assertThat(deserialized.getCurrentAttempt()).isEqualTo(original.getCurrentAttempt());
        assertThat(deserialized.getState()).isEqualTo(Job.ExecutionState.FAILED);
        assertThat(deserialized.getAssignedWorker()).isEqualTo("worker-1");
        assertThat(deserialized.getFailureReason()).isEqualTo("Connection timeout");
        assertThat(deserialized.getLeaseExpiry()).isEqualTo(original.getLeaseExpiry());
        assertThat(deserialized.getVersion()).isEqualTo(original.getVersion());
    }

    @Test
    @DisplayName("Should handle null assignedWorker")
    void testNullAssignedWorker() {
        // Given
        WorkUnitSerializer<String> serializer = new WorkUnitSerializer<>(new StringCodec());
        WorkUnit<String> original = new WorkUnit<>("job-3", "Task", System.currentTimeMillis(), 3);

        // When
        byte[] serialized = serializer.serialize(original);
        WorkUnit<String> deserialized = serializer.deserialize(serialized);

        // Then
        assertThat(deserialized.getAssignedWorker()).isNull();
    }

    @Test
    @DisplayName("Should handle null failureReason")
    void testNullFailureReason() {
        // Given
        WorkUnitSerializer<String> serializer = new WorkUnitSerializer<>(new StringCodec());
        WorkUnit<String> original = new WorkUnit<>("job-4", "Task", System.currentTimeMillis(), 3);

        // When
        byte[] serialized = serializer.serialize(original);
        WorkUnit<String> deserialized = serializer.deserialize(serialized);

        // Then
        assertThat(deserialized.getFailureReason()).isNull();
    }

    @Test
    @DisplayName("Should handle all execution states")
    void testAllStates() {
        WorkUnitSerializer<String> serializer = new WorkUnitSerializer<>(new StringCodec());
        long now = System.currentTimeMillis();

        for (Job.ExecutionState state : Job.ExecutionState.values()) {
            // Given
            WorkUnit<String> original = new WorkUnit<>("job-" + state, "Task", now, 3);
            original.setState(state);

            // When
            byte[] serialized = serializer.serialize(original);
            WorkUnit<String> deserialized = serializer.deserialize(serialized);

            // Then
            assertThat(deserialized.getState()).isEqualTo(state);
        }
    }

    @Test
    @DisplayName("Should preserve version field")
    void testVersionPreservation() {
        // Given
        WorkUnitSerializer<String> serializer = new WorkUnitSerializer<>(new StringCodec());
        WorkUnit<String> original = new WorkUnit<>("job-5", "Task", System.currentTimeMillis(), 3);

        // Modify to increment version
        original.lease("worker-1", 5000);
        original.resetForRetry();
        long expectedVersion = original.getVersion();

        // When
        byte[] serialized = serializer.serialize(original);
        WorkUnit<String> deserialized = serializer.deserialize(serialized);

        // Then
        assertThat(deserialized.getVersion()).isEqualTo(expectedVersion);
    }

    @Test
    @DisplayName("Should work with JSON codec")
    void testJsonCodec() {
        // Given
        WorkUnitSerializer<TestPayload> serializer = new WorkUnitSerializer<>(new JsonCodec<>(TestPayload.class));
        TestPayload payload = new TestPayload("test-data", 42);
        WorkUnit<TestPayload> original = new WorkUnit<>("job-6", payload, System.currentTimeMillis(), 3);

        // When
        byte[] serialized = serializer.serialize(original);
        WorkUnit<TestPayload> deserialized = serializer.deserialize(serialized);

        // Then
        assertThat(deserialized.getPayload().name).isEqualTo("test-data");
        assertThat(deserialized.getPayload().value).isEqualTo(42);
    }

    @Test
    @DisplayName("Should work with Kryo codec")
    void testKryoCodec() {
        // Given
        WorkUnitSerializer<TestPayload> serializer = new WorkUnitSerializer<>(new KryoCodec<>(TestPayload.class));
        TestPayload payload = new TestPayload("kryo-test", 99);
        WorkUnit<TestPayload> original = new WorkUnit<>("job-7", payload, System.currentTimeMillis(), 3);

        // When
        byte[] serialized = serializer.serialize(original);
        WorkUnit<TestPayload> deserialized = serializer.deserialize(serialized);

        // Then
        assertThat(deserialized.getPayload().name).isEqualTo("kryo-test");
        assertThat(deserialized.getPayload().value).isEqualTo(99);
    }

    @Test
    @DisplayName("Should estimate size correctly")
    void testSizeEstimation() {
        // Given
        WorkUnitSerializer<String> serializer = new WorkUnitSerializer<>(new StringCodec());
        WorkUnit<String> unit = new WorkUnit<>("job-8", "Test payload", System.currentTimeMillis(), 3);
        unit.lease("worker-1", 5000);

        // When
        int estimatedSize = serializer.estimateSize(unit);
        byte[] actualBytes = serializer.serialize(unit);

        // Then
        assertThat(actualBytes.length).isEqualTo(estimatedSize);
    }

    @Test
    @DisplayName("Should handle empty strings")
    void testEmptyStrings() {
        // Given
        WorkUnitSerializer<String> serializer = new WorkUnitSerializer<>(new StringCodec());
        WorkUnit<String> original = new WorkUnit<>("", "", System.currentTimeMillis(), 3);

        // When
        byte[] serialized = serializer.serialize(original);
        WorkUnit<String> deserialized = serializer.deserialize(serialized);

        // Then
        assertThat(deserialized.getId()).isEmpty();
        assertThat(deserialized.getPayload()).isEmpty();
    }

    @Test
    @DisplayName("Should handle long strings")
    void testLongStrings() {
        // Given
        WorkUnitSerializer<String> serializer = new WorkUnitSerializer<>(new StringCodec());
        String longId = "job-" + "x".repeat(1000);
        String longPayload = "payload-" + "y".repeat(10000);
        WorkUnit<String> original = new WorkUnit<>(longId, longPayload, System.currentTimeMillis(), 3);

        // When
        byte[] serialized = serializer.serialize(original);
        WorkUnit<String> deserialized = serializer.deserialize(serialized);

        // Then
        assertThat(deserialized.getId()).isEqualTo(longId);
        assertThat(deserialized.getPayload()).isEqualTo(longPayload);
    }

    @Test
    @DisplayName("Should handle Unicode strings")
    void testUnicodeStrings() {
        // Given
        WorkUnitSerializer<String> serializer = new WorkUnitSerializer<>(new StringCodec());
        String unicodeId = "job-日本語-🚀-中文";
        String unicodePayload = "Payload with émojis 😀 and special chars: ñ, é, ü";
        WorkUnit<String> original = new WorkUnit<>(unicodeId, unicodePayload, System.currentTimeMillis(), 3);

        // When
        byte[] serialized = serializer.serialize(original);
        WorkUnit<String> deserialized = serializer.deserialize(serialized);

        // Then
        assertThat(deserialized.getId()).isEqualTo(unicodeId);
        assertThat(deserialized.getPayload()).isEqualTo(unicodePayload);
    }

    @Test
    @DisplayName("Should preserve timestamps accurately")
    void testTimestampPreservation() {
        // Given
        WorkUnitSerializer<String> serializer = new WorkUnitSerializer<>(new StringCodec());
        long now = System.currentTimeMillis();
        WorkUnit<String> original = new WorkUnit<>("job-9", "Task", now, 3);

        // When
        byte[] serialized = serializer.serialize(original);
        WorkUnit<String> deserialized = serializer.deserialize(serialized);

        // Then
        assertThat(deserialized.getScheduledTime()).isEqualTo(now);
        assertThat(deserialized.getCreatedAt()).isEqualTo(original.getCreatedAt());
        assertThat(deserialized.getLastModified()).isEqualTo(original.getLastModified());
    }

    @Test
    @DisplayName("Should be deterministic (same input produces same output)")
    void testDeterminism() {
        // Given
        WorkUnitSerializer<String> serializer = new WorkUnitSerializer<>(new StringCodec());
        WorkUnit<String> unit = new WorkUnit<>("job-10", "Deterministic test", 1234567890L, 3);

        // When
        byte[] serialized1 = serializer.serialize(unit);
        byte[] serialized2 = serializer.serialize(unit);

        // Then
        assertThat(serialized1).isEqualTo(serialized2);
    }

    // Test helper class
    public static class TestPayload implements java.io.Serializable {
        public String name;
        public int value;

        public TestPayload() {}

        public TestPayload(String name, int value) {
            this.name = name;
            this.value = value;
        }
    }
}
