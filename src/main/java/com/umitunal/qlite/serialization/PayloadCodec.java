package com.umitunal.qlite.serialization;

/**
 * Interface for encoding and decoding job payloads.
 *
 * @param <T> the type of payload
 */
public interface PayloadCodec<T> {

    /**
     * Encode a payload to bytes.
     */
    byte[] encode(T payload);

    /**
     * Decode bytes to a payload.
     */
    T decode(byte[] bytes);
}
