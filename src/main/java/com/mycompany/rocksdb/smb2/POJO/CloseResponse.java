package com.mycompany.rocksdb.smb2.POJO;

import io.vertx.reactivex.core.buffer.Buffer;

public class CloseResponse {
    private short structureSize;
    private short flags;
    private int reserved;
    private long creationTime;
    private long lastAccessTime;
    private long lastWriteTime;
    private long changeTime;
    private long allocationSize;
    private long endOfFile;
    private int fileAttributes;

    public static final int STRUCTURE_SIZE = 60;

    public CloseResponse() {
        this.structureSize = STRUCTURE_SIZE;
    }

    public short getStructureSize() {
        return structureSize;
    }

    public void setStructureSize(short structureSize) {
        this.structureSize = structureSize;
    }

    public short getFlags() {
        return flags;
    }

    public void setFlags(short flags) {
        this.flags = flags;
    }

    public int getReserved() {
        return reserved;
    }

    public void setReserved(int reserved) {
        this.reserved = reserved;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public long getLastWriteTime() {
        return lastWriteTime;
    }

    public void setLastWriteTime(long lastWriteTime) {
        this.lastWriteTime = lastWriteTime;
    }

    public long getChangeTime() {
        return changeTime;
    }

    public void setChangeTime(long changeTime) {
        this.changeTime = changeTime;
    }

    public long getAllocationSize() {
        return allocationSize;
    }

    public void setAllocationSize(long allocationSize) {
        this.allocationSize = allocationSize;
    }

    public long getEndOfFile() {
        return endOfFile;
    }

    public void setEndOfFile(long endOfFile) {
        this.endOfFile = endOfFile;
    }

    public int getFileAttributes() {
        return fileAttributes;
    }

    public void setFileAttributes(int fileAttributes) {
        this.fileAttributes = fileAttributes;
    }

    public Buffer encode() {
        Buffer buffer = Buffer.buffer(STRUCTURE_SIZE);
        buffer.appendShortLE((short)STRUCTURE_SIZE);
        buffer.appendShortLE(flags);
        buffer.appendIntLE(reserved);
        buffer.appendLongLE(creationTime);
        buffer.appendLongLE(lastAccessTime);
        buffer.appendLongLE(lastWriteTime);
        buffer.appendLongLE(changeTime);
        buffer.appendLongLE(allocationSize);
        buffer.appendLongLE(endOfFile);
        buffer.appendIntLE(fileAttributes);
        return buffer;
    }

    public static CloseResponse decode(Buffer buffer, int offset) {
        CloseResponse response = new CloseResponse();
        response.structureSize = buffer.getShortLE(offset);
        response.flags = buffer.getShortLE(offset + 2);
        response.reserved = buffer.getIntLE(offset + 4);
        response.creationTime = buffer.getLongLE(offset + 8);
        response.lastAccessTime = buffer.getLongLE(offset + 16);
        response.lastWriteTime = buffer.getLongLE(offset + 24);
        response.changeTime = buffer.getLongLE(offset + 32);
        response.allocationSize = buffer.getLongLE(offset + 40);
        response.endOfFile = buffer.getLongLE(offset + 48);
        response.fileAttributes = buffer.getIntLE(offset + 56);
        return response;
    }
}

