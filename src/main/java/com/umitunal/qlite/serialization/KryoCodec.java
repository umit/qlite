package com.umitunal.qlite.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayOutputStream;

/**
 * High-performance serialization using Kryo.
 * Kryo is much faster than Java serialization and JSON.
 *
 * Features:
 * - Zero-copy optimization
 * - Compact binary format
 * - No reflection overhead after first use
 * - Supports complex object graphs
 *
 * @param <T> the type to serialize
 */
public class KryoCodec<T> implements PayloadCodec<T> {
    private final ThreadLocal<Kryo> kryoThreadLocal;
    private final Class<T> type;

    public KryoCodec(Class<T> type) {
        this.type = type;
        this.kryoThreadLocal = ThreadLocal.withInitial(() -> {
            Kryo kryo = new Kryo();
            // Don't require registration for flexibility
            kryo.setRegistrationRequired(false);
            // Handle circular references
            kryo.setReferences(true);
            return kryo;
        });
    }

    /**
     * Create a Kryo codec with custom Kryo instance configuration.
     */
    public KryoCodec(Class<T> type, KryoFactory factory) {
        this.type = type;
        this.kryoThreadLocal = ThreadLocal.withInitial(factory::create);
    }

    @Override
    public byte[] encode(T payload) {
        Kryo kryo = kryoThreadLocal.get();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (Output output = new Output(baos)) {
            kryo.writeObject(output, payload);
            output.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public T decode(byte[] bytes) {
        Kryo kryo = kryoThreadLocal.get();

        try (Input input = new Input(bytes)) {
            return kryo.readObject(input, type);
        }
    }

    /**
     * Factory interface for custom Kryo configuration.
     */
    @FunctionalInterface
    public interface KryoFactory {
        Kryo create();
    }

    /**
     * Pre-configured factory for common use cases.
     */
    public static class Factories {

        /**
         * Default factory - balanced between flexibility and performance.
         */
        public static Kryo defaultFactory() {
            Kryo kryo = new Kryo();
            kryo.setRegistrationRequired(false);
            kryo.setReferences(true);
            return kryo;
        }

        /**
         * High-performance factory - requires class registration but fastest.
         */
        public static Kryo performanceFactory() {
            Kryo kryo = new Kryo();
            kryo.setRegistrationRequired(true);
            kryo.setReferences(false);
            return kryo;
        }

        /**
         * Safe factory - handles complex object graphs with cycles.
         */
        public static Kryo safeFactory() {
            Kryo kryo = new Kryo();
            kryo.setRegistrationRequired(false);
            kryo.setReferences(true);
            kryo.setWarnUnregisteredClasses(true);
            return kryo;
        }
    }
}
