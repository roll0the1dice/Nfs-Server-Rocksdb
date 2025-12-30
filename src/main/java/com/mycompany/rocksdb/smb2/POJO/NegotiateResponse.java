package com.mycompany.rocksdb.smb2.POJO;

import com.mycompany.rocksdb.smb2.Smb2Constants;
import com.mycompany.rocksdb.smb2.Smb2LittleEndianUtils;
import com.mycompany.rocksdb.smb2.Smb2Utils;

import io.vertx.reactivex.core.buffer.Buffer;

// SMB2 NEGOTIATE Response Structure (section 2.2.4)
public class NegotiateResponse {
    public static final int STRUCTURE_SIZE = 64;

    private short structureSize; // 2 bytes
    private short securityMode; // 2 bytes
    private short dialectRevision; // 2 bytes
    private short reserved; // 2 bytes
    private byte[] serverGuid; // 16 bytes
    private long capabilities; // 4 bytes
    private long maxTransactSize; // 4 bytes
    private long maxReadSize; // 4 bytes
    private long maxWriteSize; // 4 bytes
    private long systemTime; // 8 bytes (FILETIME)
    private long serverStartTime; // 8 bytes (FILETIME)
    private int securityBufferOffset; // 2 bytes (offset from start of response)
    private int securityBufferLength; // 2 bytes
    private long negotiateContextOffset; // 4 bytes (offset from start of response)

    private byte[] securityBuffer; // Variable length (for GSSAPI/SPNEGO token)
    // TODO: Add NegotiateContextList if needed

    public NegotiateResponse() {
        this.structureSize = STRUCTURE_SIZE;
        this.serverGuid = new byte[16];
        this.securityBuffer = new byte[0];
    }

    public Buffer encode() {
        Buffer buffer = Buffer.buffer();

        Smb2Utils.writeShort(buffer, structureSize);
        Smb2Utils.writeShort(buffer, securityMode);
        Smb2Utils.writeShort(buffer, dialectRevision);
        Smb2Utils.writeShort(buffer, reserved);
        Smb2Utils.writeBytes(buffer, serverGuid);
        Smb2Utils.writeInt(buffer, capabilities);
        Smb2Utils.writeInt(buffer, maxTransactSize);
        Smb2Utils.writeInt(buffer, maxReadSize);
        Smb2Utils.writeInt(buffer, maxWriteSize);
        Smb2Utils.writeLong(buffer, systemTime);
        Smb2Utils.writeLong(buffer, serverStartTime);
        // Calculate offsets based on current buffer length
        this.securityBufferOffset = Smb2Header.STRUCTURE_SIZE + buffer.length(); // Header + Negotiate Response fixed part
        Smb2Utils.writeShort(buffer, securityBufferOffset);
        Smb2Utils.writeShort(buffer, securityBuffer.length);
        Smb2Utils.writeInt(buffer, negotiateContextOffset); // Placeholder for now

        Smb2Utils.writeBytes(buffer, securityBuffer);

        return buffer;
    }

    // Getters and Setters
    public short getStructureSize() {
        return structureSize;
    }

    public void setStructureSize(short structureSize) {
        this.structureSize = structureSize;
    }

    public short getSecurityMode() {
        return securityMode;
    }

    public void setSecurityMode(short securityMode) {
        this.securityMode = securityMode;
    }

    public short getDialectRevision() {
        return dialectRevision;
    }

    public void setDialectRevision(short dialectRevision) {
        this.dialectRevision = dialectRevision;
    }

    public short getReserved() {
        return reserved;
    }

    public void setReserved(short reserved) {
        this.reserved = reserved;
    }

    public byte[] getServerGuid() {
        return serverGuid;
    }

    public void setServerGuid(byte[] serverGuid) {
        this.serverGuid = serverGuid;
    }

    public long getCapabilities() {
        return capabilities;
    }

    public void setCapabilities(long capabilities) {
        this.capabilities = capabilities;
    }

    public long getMaxTransactSize() {
        return maxTransactSize;
    }

    public void setMaxTransactSize(long maxTransactSize) {
        this.maxTransactSize = maxTransactSize;
    }

    public long getMaxReadSize() {
        return maxReadSize;
    }

    public void setMaxReadSize(long maxReadSize) {
        this.maxReadSize = maxReadSize;
    }

    public long getMaxWriteSize() {
        return maxWriteSize;
    }

    public void setMaxWriteSize(long maxWriteSize) {
        this.maxWriteSize = maxWriteSize;
    }

    public long getSystemTime() {
        return systemTime;
    }

    public void setSystemTime(long systemTime) {
        this.systemTime = systemTime;
    }

    public long getServerStartTime() {
        return serverStartTime;
    }

    public void setServerStartTime(long serverStartTime) {
        this.serverStartTime = serverStartTime;
    }

    public int getSecurityBufferOffset() {
        return securityBufferOffset;
    }

    public void setSecurityBufferOffset(int securityBufferOffset) {
        this.securityBufferOffset = securityBufferOffset;
    }

    public int getSecurityBufferLength() {
        return securityBufferLength;
    }

    public void setSecurityBufferLength(int securityBufferLength) {
        this.securityBufferLength = securityBufferLength;
    }

    public long getNegotiateContextOffset() {
        return negotiateContextOffset;
    }

    public void setNegotiateContextOffset(long negotiateContextOffset) {
        this.negotiateContextOffset = negotiateContextOffset;
    }

    public byte[] getSecurityBuffer() {
        return securityBuffer;
    }

    public void setSecurityBuffer(byte[] securityBuffer) {
        this.securityBuffer = securityBuffer;
    }
}

