package com.mycompany.rocksdb.smb2.POJO;

import com.mycompany.rocksdb.smb2.Smb2LittleEndianUtils;
import com.mycompany.rocksdb.smb2.Smb2Utils;

import io.vertx.reactivex.core.buffer.Buffer;
import lombok.Data;

// SMB2 CREATE Response Structure (section 2.2.14)
@Data
public class CreateResponse {
    public static final int STRUCTURE_SIZE = 80;

    private short structureSize; // 2 bytes
    private byte oplockLevel; // 1 byte
    private byte flags; // 1 byte
    private long createAction; // 4 bytes
    private long creationTime; // 8 bytes (FILETIME)
    private long lastAccessTime; // 8 bytes (FILETIME)
    private long lastWriteTime; // 8 bytes (FILETIME)
    private long changeTime; // 8 bytes (FILETIME)
    private long fileAttributes; // 4 bytes
    private long allocationSize; // 8 bytes
    private long endOfFile; // 8 bytes
    private short fileIdVolatile; // 8 bytes
    private long fileIdPersistent; // 8 bytes
    private long fileGuidHandleHigh; // 8 bytes
    private long fileGuidHandleLow; // 8 bytes
    private int createContextsOffset; // 4 bytes
    private int createContextsLength; // 4 bytes

    // TODO: Add CreateContexts if needed

    public CreateResponse() {
        this.structureSize = STRUCTURE_SIZE;
    }

    public Buffer encode() {
        Buffer buffer = Buffer.buffer();

        Smb2Utils.writeShortLE(buffer, structureSize);
        buffer.appendByte(oplockLevel);
        buffer.appendByte(flags);
        Smb2Utils.writeIntLE(buffer, (int) createAction);
        Smb2Utils.writeLongLE(buffer, creationTime);
        Smb2Utils.writeLongLE(buffer, lastAccessTime);
        Smb2Utils.writeLongLE(buffer, lastWriteTime);
        Smb2Utils.writeLongLE(buffer, changeTime);
        Smb2Utils.writeLongLE(buffer, allocationSize);
        Smb2Utils.writeLongLE(buffer, endOfFile);   
        Smb2Utils.writeIntLE(buffer, (int) fileAttributes);
        Smb2Utils.writeIntLE(buffer, (int) 0x00000000); // Reserved
        Smb2Utils.writeLongLE(buffer, fileIdPersistent); // 8 bytes 跨连接/会话持久存在的 ID（通常对应 Inode 或文件索引号）。
        Smb2Utils.writeLongLE(buffer, fileIdVolatile); // 8 bytes  仅在当前句柄生命周期内有效的 ID（通常由服务端生成的内存映射号）。
        // Calculate offsets based on current buffer length
        // Smb2Utils.writeLongLE(buffer, fileGuidHandleLow); // 8 bytes
        // Smb2Utils.writeLongLE(buffer, fileGuidHandleHigh); // 8 bytes
        //this.createContextsOffset = Smb2Header.STRUCTURE_SIZE + buffer.length();
        Smb2Utils.writeIntLE(buffer, createContextsOffset);
        Smb2Utils.writeIntLE(buffer, createContextsLength);

        // TODO: Write CreateContexts if any

        return buffer;
    }

    // Getters and Setters
    public short getStructureSize() {
        return structureSize;
    }

    public void setStructureSize(short structureSize) {
        this.structureSize = structureSize;
    }

    public byte getOplockLevel() {
        return oplockLevel;
    }

    public void setOplockLevel(byte oplockLevel) {
        this.oplockLevel = oplockLevel;
    }

    public byte getFlags() {
        return flags;
    }

    public void setFlags(byte flags) {
        this.flags = flags;
    }

    public long getCreateAction() {
        return createAction;
    }

    public void setCreateAction(long createAction) {
        this.createAction = createAction;
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

    public long getFileAttributes() {
        return fileAttributes;
    }

    public void setFileAttributes(long fileAttributes) {
        this.fileAttributes = fileAttributes;
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

    public long getFileIdVolatile() {
        return fileIdVolatile;
    }

    public void setFileIdVolatile(short fileIdVolatile) {
        this.fileIdVolatile = fileIdVolatile;
    }

    public long getFileIdPersistent() {
        return fileIdPersistent;
    }

    public void setFileIdPersistent(long fileIdPersistent) {
        this.fileIdPersistent = fileIdPersistent;
    }

    public int getCreateContextsOffset() {
        return createContextsOffset;
    }

    public void setCreateContextsOffset(int createContextsOffset) {
        this.createContextsOffset = createContextsOffset;
    }

    public int getCreateContextsLength() {
        return createContextsLength;
    }

    public void setCreateContextsLength(int createContextsLength) {
        this.createContextsLength = createContextsLength;
    }
}

