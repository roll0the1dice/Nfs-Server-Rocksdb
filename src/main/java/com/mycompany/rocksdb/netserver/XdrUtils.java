package com.mycompany.rocksdb.netserver;

import java.nio.charset.StandardCharsets;

import io.vertx.reactivex.core.buffer.Buffer;

public class XdrUtils {

    // XDR encodes integers as 4 bytes, big-endian.
    public static int readInt(Buffer buffer, int offset) {
        return buffer.getInt(offset);
    }

    public static void writeInt(Buffer buffer, int value) {
        buffer.appendInt(value);
    }

    public static void writeLong(Buffer buffer, long value) {
        buffer.appendLong(value);
    }

    public static void writeBoolean(Buffer buffer, boolean value) {
        buffer.appendInt(value ? 1 : 0);
    }

    public static void writeXdrString(Buffer buffer, String val) {
        if (val == null) val = "";
        byte[] bytes = val.getBytes(StandardCharsets.UTF_8);

        // 1. 必须先写 4字节的长度！
        XdrUtils.writeInt(buffer, bytes.length);

        // 2. 写入字符串内容
        buffer.appendBytes(bytes);

        // 3. 必须计算 Padding (对齐到4字节)
        // 比如 "root" 长度4，padding=0
        // 比如 "nfs"  长度3，padding=1
        int padding = (4 - (bytes.length % 4)) % 4;
        for (int i = 0; i < padding; i++) {
            buffer.appendByte((byte) 0);
        }
    }

    // XDR opaque data: length (4 bytes) + data (padded to a multiple of 4 bytes)
    public static byte[] readOpaque(Buffer buffer, int offset) {
        int length = readInt(buffer, offset);
        offset += 4; // Move past the length field

        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = buffer.getByte(offset + i);
        }
        return data;
    }

    public static void writeOpaque(Buffer buffer, byte[] data) {
        if (data == null || data.length == 0) {
            writeInt(buffer, 0);
            return;
        }
        writeInt(buffer, data.length);

        buffer.appendBytes(data);
        // Pad to a multiple of 4 bytes
        int padding = (4 - (data.length % 4)) % 4;
        for (int i = 0; i < padding; i++) {
            buffer.appendByte((byte) 0);
        }
    }

    public static void writeBytesWithoutLength(Buffer buffer, byte[] data) {
        if (data == null || data.length == 0) {
            writeInt(buffer, 0);
            return;
        }

        buffer.appendBytes(data);
    }

    public static void writeXdrPad(Buffer buffer, int length) {
        int padding = (4 - (length % 4)) % 4;
        for (int i = 0; i < padding; i++) {
            buffer.appendByte((byte) 0);
        }
    }

    // Helper to read unsigned int as long (since Java ints are signed)
    public static long readUnsignedInt(Buffer buffer, int offset) {
        return buffer.getUnsignedInt(offset);
    }

    public static int getOpaquePaddedLength(int length) {
        return length + (4 - (length % 4)) % 4;
    }

    // public static void writeXdrString(Buffer buffer, String value) {
    //     if (value == null) {
    //         writeInt(buffer, 0); // Length of 0 for null string
    //         return;
    //     }
    //     byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    //     writeInt(buffer, bytes.length);
    //     buffer.appendBytes(bytes);
    //     writeXdrPad(buffer, bytes.length);
    // }
}
