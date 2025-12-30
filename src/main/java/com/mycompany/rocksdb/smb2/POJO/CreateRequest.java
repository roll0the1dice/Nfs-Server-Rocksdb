package com.mycompany.rocksdb.smb2.POJO;

import com.mycompany.rocksdb.smb2.Smb2LittleEndianUtils;
import com.mycompany.rocksdb.smb2.Smb2Utils;

import io.vertx.reactivex.core.buffer.Buffer;

// SMB2 CREATE Request Structure (section 2.2.13)
public class CreateRequest {
    public static final int STRUCTURE_SIZE = 57;

    private short structureSize; // 2 bytes
    private byte securityFlags; // 1 byte
    private byte requestedOplockLevel; // 1 byte
    private int impersonationLevel; // 4 bytes
    private long createFlags; // 8 bytes
    private long rootDirectoryFid; // 8 bytes
    private long desiredAccess; // 4 bytes
    private long fileAttributes; // 4 bytes
    private long shareAccess; // 4 bytes
    private long createDisposition; // 4 bytes
    private long createOptions; // 4 bytes
    private int nameOffset; // 2 bytes (offset from start of SMB2 header)
    private int nameLength; // 2 bytes
    private int createContextsOffset; // 4 bytes (offset from start of SMB2 header)
    private int createContextsLength; // 4 bytes

    private String fileName; // Variable length UTF-16LE string
    // TODO: Add CreateContexts if needed

    public CreateRequest() {
        this.structureSize = STRUCTURE_SIZE;
    }

    public static CreateRequest decode(Buffer buffer, int offset) {
        CreateRequest request = new CreateRequest();

        request.setStructureSize((short) Smb2Utils.readShortLE(buffer, offset));
        offset += 2;
        request.setSecurityFlags(buffer.getByte(offset));
        offset += 1;
        request.setRequestedOplockLevel(buffer.getByte(offset));
        offset += 1;
        request.setImpersonationLevel(Smb2Utils.readIntLE(buffer, offset));
        offset += 4;
        request.setCreateFlags(Smb2Utils.readLongLE(buffer, offset)); // 8 bytes
        offset += 8;
        request.setRootDirectoryFid(Smb2Utils.readLongLE(buffer, offset)); // 8 bytes
        offset += 8;
        request.setDesiredAccess(Smb2Utils.readIntLE(buffer, offset));
        offset += 4;
        request.setFileAttributes(Smb2Utils.readIntLE(buffer, offset));
        offset += 4;
        request.setShareAccess(Smb2Utils.readIntLE(buffer, offset));
        offset += 4;
        request.setCreateDisposition(Smb2Utils.readIntLE(buffer, offset));
        offset += 4;
        request.setCreateOptions(Smb2Utils.readIntLE(buffer, offset));
        offset += 4;
        request.setNameOffset(Smb2Utils.readShortLE(buffer, offset));
        offset += 2;
        request.setNameLength(Smb2Utils.readShortLE(buffer, offset));
        offset += 2;
        request.setCreateContextsOffset(Smb2Utils.readIntLE(buffer, offset));
        offset += 4;
        request.setCreateContextsLength(Smb2Utils.readIntLE(buffer, offset));
        offset += 4;

        // The NameOffset is relative to the start of the SMB2 header
        int actualNameOffset = request.getNameOffset();
        if (request.getNameLength() > 0 && actualNameOffset + request.getNameLength() <= buffer.length()) {
            request.setFileName(Smb2Utils.readSmb2String(buffer, actualNameOffset, request.getNameLength()));
        }

        return request;
    }

    // Getters and Setters
    public short getStructureSize() {
        return structureSize;
    }

    public void setStructureSize(short structureSize) {
        this.structureSize = structureSize;
    }

    public byte getSecurityFlags() {
        return securityFlags;
    }

    public void setSecurityFlags(byte securityFlags) {
        this.securityFlags = securityFlags;
    }

    public byte getRequestedOplockLevel() {
        return requestedOplockLevel;
    }

    public void setRequestedOplockLevel(byte requestedOplockLevel) {
        this.requestedOplockLevel = requestedOplockLevel;
    }

    public int getImpersonationLevel() {
        return impersonationLevel;
    }

    public void setImpersonationLevel(int impersonationLevel) {
        this.impersonationLevel = impersonationLevel;
    }

    public long getCreateFlags() {
        return createFlags;
    }

    public void setCreateFlags(long createFlags) {
        this.createFlags = createFlags;
    }

    public long getRootDirectoryFid() {
        return rootDirectoryFid;
    }

    public void setRootDirectoryFid(long rootDirectoryFid) {
        this.rootDirectoryFid = rootDirectoryFid;
    }

    public long getDesiredAccess() {
        return desiredAccess;
    }

    public void setDesiredAccess(long desiredAccess) {
        this.desiredAccess = desiredAccess;
    }

    public long getFileAttributes() {
        return fileAttributes;
    }

    public void setFileAttributes(long fileAttributes) {
        this.fileAttributes = fileAttributes;
    }

    public long getShareAccess() {
        return shareAccess;
    }

    public void setShareAccess(long shareAccess) {
        this.shareAccess = shareAccess;
    }

    public long getCreateDisposition() {
        return createDisposition;
    }

    public void setCreateDisposition(long createDisposition) {
        this.createDisposition = createDisposition;
    }

    public long getCreateOptions() {
        return createOptions;
    }

    public void setCreateOptions(long createOptions) {
        this.createOptions = createOptions;
    }

    public int getNameOffset() {
        return nameOffset;
    }

    public void setNameOffset(int nameOffset) {
        this.nameOffset = nameOffset;
    }

    public int getNameLength() {
        return nameLength;
    }

    public void setNameLength(int nameLength) {
        this.nameLength = nameLength;
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

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}

