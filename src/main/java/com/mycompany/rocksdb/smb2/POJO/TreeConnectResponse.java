package com.mycompany.rocksdb.smb2.POJO;

import com.mycompany.rocksdb.smb2.Smb2LittleEndianUtils;
import com.mycompany.rocksdb.smb2.Smb2Utils;

import io.vertx.reactivex.core.buffer.Buffer;
import lombok.Data;

// SMB2 TREE_CONNECT Response Structure (section 2.2.10)
@Data
public class TreeConnectResponse {
    public static final int STRUCTURE_SIZE = 16;

    private short structureSize; // 2 bytes
    private short shareType;
    private short shareFlags; // 2 bytes
    private int capabilities; // 4 bytes
    private int maximalAccess; // 4 bytes
    private long reserved; // 4 bytes

    public TreeConnectResponse() {
        this.structureSize = STRUCTURE_SIZE;
    }

    public Buffer encode() {
        Buffer buffer = Buffer.buffer();

        Smb2Utils.writeShortLE(buffer, structureSize);
        Smb2Utils.writeIntLE(buffer, shareType); // Example shareFlags: FILE_SHARE_READ | FILE_SHARE_WRITE
        Smb2Utils.writeIntLE(buffer, shareFlags);
        Smb2Utils.writeIntLE(buffer, capabilities);
        Smb2Utils.writeIntLE(buffer, maximalAccess);

        return buffer;
    }

    // Getters and Setters
    public short getStructureSize() {
        return structureSize;
    }

    public void setStructureSize(short structureSize) {
        this.structureSize = structureSize;
    }

    public short getShareFlags() {
        return shareFlags;
    }

    public void setShareFlags(short shareFlags) {
        this.shareFlags = shareFlags;
    }

    public int getCapabilities() {
        return capabilities;
    }

    public void setCapabilities(int capabilities) {
        this.capabilities = capabilities;
    }

    public int getMaximalAccess() {
        return maximalAccess;
    }

    public void setMaximalAccess(int maximalAccess) {
        this.maximalAccess = maximalAccess;
    }

    public long getReserved() {
        return reserved;
    }

    public void setReserved(long reserved) {
        this.reserved = reserved;
    }
}

