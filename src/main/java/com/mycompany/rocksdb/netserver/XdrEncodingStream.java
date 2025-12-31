package com.mycompany.rocksdb.netserver;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class XdrEncodingStream {
    private final ByteArrayOutputStream baos;

    public XdrEncodingStream() {
        this.baos = new ByteArrayOutputStream();
    }

    public void writeInt(int value) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.order(ByteOrder.BIG_ENDIAN);
        bb.putInt(value);
        try {
            baos.write(bb.array());
        } catch (IOException e) {
            throw new RuntimeException("Failed to write int to XDR stream", e);
        }
    }

    public void writeUnsignedInt(long value) {
        writeInt((int) (value & 0xFFFFFFFFL));
    }

    public void writeOpaque(byte[] data) {
        writeInt(data.length);
        try {
            baos.write(data);
            int padding = (4 - (data.length % 4)) % 4;
            for (int i = 0; i < padding; i++) {
                baos.write(0);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write opaque data to XDR stream", e);
        }
    }

    public void writeBytes(byte[] data) {
        try {
            baos.write(data);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write bytes to XDR stream", e);
        }
    }

    public byte[] toByteArray() {
        return baos.toByteArray();
    }
}
