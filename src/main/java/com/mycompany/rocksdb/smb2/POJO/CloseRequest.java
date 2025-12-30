package com.mycompany.rocksdb.smb2.POJO;

import io.vertx.reactivex.core.buffer.Buffer;

public class CloseRequest {
    private short structureSize;
    private short flags;
    private int reserved;
    private long fileIdVolatile;
    private long fileIdPersistent;

    public static final int STRUCTURE_SIZE = 24;

    public CloseRequest() {
        this.structureSize = STRUCTURE_SIZE;
    }

    public short getStructureSize() {
        return structureSize;
    }

    public short getFlags() {
        return flags;
    }

    public int getReserved() {
        return reserved;
    }

    public long getFileIdVolatile() {
        return fileIdVolatile;
    }

    public long getFileIdPersistent() {
        return fileIdPersistent;
    }

    public static CloseRequest decode(Buffer buffer, int offset) {
        CloseRequest request = new CloseRequest();
        request.structureSize = buffer.getShortLE(offset);
        request.flags = buffer.getShortLE(offset + 2);
        request.reserved = buffer.getIntLE(offset + 4);
        request.fileIdPersistent = buffer.getLongLE(offset + 8);
        request.fileIdVolatile = buffer.getLongLE(offset + 16);
        return request;
    }

    public Buffer encode() {
        Buffer buffer = Buffer.buffer(STRUCTURE_SIZE);
        buffer.appendShortLE(structureSize);
        buffer.appendShortLE(flags);
        buffer.appendIntLE(reserved);
        buffer.appendLongLE(fileIdVolatile);
        buffer.appendLongLE(fileIdPersistent);
        return buffer;
    }
}

