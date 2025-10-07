package com.umitunal.qlite.serialization;

import com.esotericsoftware.kryo.Kryo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class KryoCodecTest {

    @Test
    @DisplayName("Should encode and decode simple strings")
    void testSimpleString() {
        // Given
        KryoCodec<String> codec = new KryoCodec<>(String.class);
        String original = "Hello, Kryo!";

        // When
        byte[] encoded = codec.encode(original);
        String decoded = codec.decode(encoded);

        // Then
        assertThat(decoded).isEqualTo(original);
    }

    @Test
    @DisplayName("Should encode and decode complex objects")
    void testComplexObject() {
        // Given
        KryoCodec<TestObject> codec = new KryoCodec<>(TestObject.class);
        TestObject original = new TestObject(
                "test-id",
                42,
                List.of("item1", "item2", "item3"),
                Map.of("key1", "value1", "key2", "value2")
        );

        // When
        byte[] encoded = codec.encode(original);
        TestObject decoded = codec.decode(encoded);

        // Then
        assertThat(decoded.id).isEqualTo(original.id);
        assertThat(decoded.count).isEqualTo(original.count);
        assertThat(decoded.items).containsExactlyElementsOf(original.items);
        assertThat(decoded.metadata).containsAllEntriesOf(original.metadata);
    }

    @Test
    @DisplayName("Should handle nested objects")
    void testNestedObjects() {
        // Given
        KryoCodec<NestedObject> codec = new KryoCodec<>(NestedObject.class);
        NestedObject original = new NestedObject(
                "parent",
                new TestObject("child", 10, List.of("a", "b"), Map.of())
        );

        // When
        byte[] encoded = codec.encode(original);
        NestedObject decoded = codec.decode(encoded);

        // Then
        assertThat(decoded.name).isEqualTo(original.name);
        assertThat(decoded.child.id).isEqualTo(original.child.id);
        assertThat(decoded.child.count).isEqualTo(original.child.count);
    }

    @Test
    @DisplayName("Should handle circular references")
    void testCircularReferences() {
        // Given
        KryoCodec<CircularObject> codec = new KryoCodec<>(CircularObject.class);
        CircularObject obj1 = new CircularObject("obj1");
        CircularObject obj2 = new CircularObject("obj2");
        obj1.reference = obj2;
        obj2.reference = obj1;

        // When
        byte[] encoded = codec.encode(obj1);
        CircularObject decoded = codec.decode(encoded);

        // Then
        assertThat(decoded.name).isEqualTo("obj1");
        assertThat(decoded.reference.name).isEqualTo("obj2");
        assertThat(decoded.reference.reference.name).isEqualTo("obj1");
    }

    @Test
    @DisplayName("Should work with custom Kryo factory")
    void testCustomFactory() {
        // Given
        KryoCodec<TestObject> codec = new KryoCodec<>(TestObject.class, () -> {
            Kryo kryo = new Kryo();
            kryo.setRegistrationRequired(false);
            kryo.setReferences(false); // Disable for simple objects
            kryo.register(TestObject.class);
            return kryo;
        });

        TestObject original = new TestObject("test", 99, List.of("x"), Map.of());

        // When
        byte[] encoded = codec.encode(original);
        TestObject decoded = codec.decode(encoded);

        // Then
        assertThat(decoded.id).isEqualTo(original.id);
        assertThat(decoded.count).isEqualTo(original.count);
    }

    @Test
    @DisplayName("Should be thread-safe")
    void testThreadSafety() throws InterruptedException {
        // Given
        KryoCodec<String> codec = new KryoCodec<>(String.class);
        int threadCount = 10;
        int iterations = 100;
        Thread[] threads = new Thread[threadCount];

        // When
        for (int i = 0; i < threadCount; i++) {
            final int threadNum = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < iterations; j++) {
                    String original = "Thread-" + threadNum + "-Iteration-" + j;
                    byte[] encoded = codec.encode(original);
                    String decoded = codec.decode(encoded);
                    assertThat(decoded).isEqualTo(original);
                }
            });
            threads[i].start();
        }

        // Then - All threads complete without error
        for (Thread thread : threads) {
            thread.join();
        }
    }

    @Test
    @DisplayName("Should produce compact binary format")
    void testCompactFormat() {
        // Given
        KryoCodec<TestObject> codec = new KryoCodec<>(TestObject.class);
        TestObject obj = new TestObject(
                "very-long-identifier-string",
                12345,
                List.of("a", "b", "c", "d", "e"),
                Map.of("k1", "v1", "k2", "v2", "k3", "v3")
        );

        // When
        byte[] encoded = codec.encode(obj);

        // Then - Should be reasonably compact
        assertThat(encoded.length).isLessThan(300); // Rough estimate
    }

    @Test
    @DisplayName("Should handle null values")
    void testNullValues() {
        // Given
        KryoCodec<TestObject> codec = new KryoCodec<>(TestObject.class);
        TestObject original = new TestObject(null, 0, null, null);

        // When
        byte[] encoded = codec.encode(original);
        TestObject decoded = codec.decode(encoded);

        // Then
        assertThat(decoded.id).isNull();
        assertThat(decoded.count).isEqualTo(0);
        assertThat(decoded.items).isNull();
        assertThat(decoded.metadata).isNull();
    }

    @Test
    @DisplayName("Should handle empty collections")
    void testEmptyCollections() {
        // Given
        KryoCodec<TestObject> codec = new KryoCodec<>(TestObject.class);
        TestObject original = new TestObject("id", 0, List.of(), Map.of());

        // When
        byte[] encoded = codec.encode(original);
        TestObject decoded = codec.decode(encoded);

        // Then
        assertThat(decoded.items).isEmpty();
        assertThat(decoded.metadata).isEmpty();
    }

    // Test helper classes
    public static class TestObject implements Serializable {
        private String id;
        private int count;
        private List<String> items;
        private Map<String, String> metadata;

        public TestObject() {}

        public TestObject(String id, int count, List<String> items, Map<String, String> metadata) {
            this.id = id;
            this.count = count;
            this.items = items;
            this.metadata = metadata;
        }

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }

        public int getCount() { return count; }
        public void setCount(int count) { this.count = count; }

        public List<String> getItems() { return items; }
        public void setItems(List<String> items) { this.items = items; }

        public Map<String, String> getMetadata() { return metadata; }
        public void setMetadata(Map<String, String> metadata) { this.metadata = metadata; }
    }

    public static class NestedObject implements Serializable {
        private String name;
        private TestObject child;

        public NestedObject() {}

        public NestedObject(String name, TestObject child) {
            this.name = name;
            this.child = child;
        }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public TestObject getChild() { return child; }
        public void setChild(TestObject child) { this.child = child; }
    }

    public static class CircularObject implements Serializable {
        private String name;
        private CircularObject reference;

        public CircularObject() {}

        public CircularObject(String name) {
            this.name = name;
        }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public CircularObject getReference() { return reference; }
        public void setReference(CircularObject reference) { this.reference = reference; }
    }
}
