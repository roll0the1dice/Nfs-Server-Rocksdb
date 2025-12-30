package com.mycompany.rocksdb.netserver;

import io.vertx.reactivex.core.buffer.Buffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class XdrDecodingStream {
    private final ByteBuffer buffer;

    public XdrDecodingStream(byte[] bytes) {
        this.buffer = ByteBuffer.wrap(bytes);
        this.buffer.order(ByteOrder.BIG_ENDIAN); // XDR is big-endian
    }

    public int readInt() {
        return buffer.getInt();
    }

<<<<<<< HEAD
    public boolean readBoolean() {
        return buffer.getInt() == 1;
    }

=======
>>>>>>> 82c35694aa92253fd9c7d0c5119e5e75ab8825be
    public long readLong() {
        return buffer.getLong();
    }

    public long readUnsignedInt() {
        return buffer.getInt() & 0xFFFFFFFFL;
    }

    public byte[] readOpaque() {
        int length = readInt();
        byte[] data = new byte[length];
        buffer.get(data);
        int padding = (4 - (length % 4)) % 4;
        buffer.position(buffer.position() + padding); // Skip padding bytes
        return data;
    }

    public byte[] readOpaque(int expectedLength) {
        byte[] data = new byte[expectedLength];
        buffer.get(data);
        int padding = (4 - (expectedLength % 4)) % 4;
        buffer.position(buffer.position() + padding); // Skip padding bytes
        return data;
    }

    public int getPosition() {
        return buffer.position();
    }

    public int getRemaining() {
        return buffer.remaining();
    }

    public Buffer readFixedOpaque(int length) {
        byte[] data = new byte[length];
        buffer.get(data);
        int padding = (4 - (length % 4)) % 4;
        buffer.position(buffer.position() + padding); // Skip padding bytes
        return Buffer.buffer(data);
    }
}
