package com.umitunal.qlite.serialization;

/**
 * Pass-through codec for raw byte arrays.
 * Useful when payload is already serialized.
 */
public class ByteArrayCodec implements PayloadCodec<byte[]> {

    @Override
    public byte[] encode(byte[] payload) {
        return payload;
    }

    @Override
    public byte[] decode(byte[] bytes) {
        return bytes;
    }
}
