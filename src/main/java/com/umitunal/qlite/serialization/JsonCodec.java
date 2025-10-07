package com.umitunal.qlite.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;

/**
 * JSON codec using Jackson for serializing complex objects.
 *
 * @param <T> the type to serialize
 */
public class JsonCodec<T> implements PayloadCodec<T> {
    private final ObjectMapper mapper;
    private final Class<T> type;

    public JsonCodec(Class<T> type) {
        this(type, createDefaultMapper());
    }

    public JsonCodec(Class<T> type, ObjectMapper mapper) {
        this.type = type;
        this.mapper = mapper;
    }

    @Override
    public byte[] encode(T payload) {
        try {
            return mapper.writeValueAsBytes(payload);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize to JSON", e);
        }
    }

    @Override
    public T decode(byte[] bytes) {
        try {
            return mapper.readValue(bytes, type);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize from JSON", e);
        }
    }

    private static ObjectMapper createDefaultMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        return mapper;
    }
}
