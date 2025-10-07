package com.umitunal.qlite.serialization;

import java.nio.charset.StandardCharsets;

/**
 * Codec for String payloads using UTF-8 encoding.
 */
public class StringCodec implements PayloadCodec<String> {

    @Override
    public byte[] encode(String payload) {
        return payload.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String decode(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
