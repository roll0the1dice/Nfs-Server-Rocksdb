package com.mycompany.rocksdb.smb2;
import io.vertx.reactivex.core.buffer.Buffer;

public class Smb2LittleEndianUtils {

    public static short readUInt16(Buffer buffer, int offset) {
        return buffer.getShort(offset);
    }

    public static int readUInt32(Buffer buffer, int offset) {
        return buffer.getInt(offset);
    }

    public static long readUInt64(Buffer buffer, int offset) {
        return buffer.getLong(offset);
    }

    public static void writeUInt16(Buffer buffer, int offset, short value) {
        buffer.setShort(offset, value);
    }

    public static void writeUInt32(Buffer buffer, int offset, int value) {
        buffer.setInt(offset, value);
    }

    public static void writeUInt64(Buffer buffer, int offset, long value) {
        buffer.setLong(offset, value);
    }

    public static byte readByte(Buffer buffer, int offset) {
        return buffer.getByte(offset);
    }

    public static void writeByte(Buffer buffer, int offset, byte value) {
        buffer.setByte(offset, value);
    }

    public static byte[] readBytes(Buffer buffer, int offset, int length) {
        return buffer.getBytes(offset, offset + length);
    }

    public static void writeBytes(Buffer buffer, int offset, byte[] bytes) {
        buffer.setBytes(offset, bytes);
    }

}
