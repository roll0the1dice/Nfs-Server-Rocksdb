package com.mycompany.rocksdb.smb2.POJO;

import com.mycompany.rocksdb.smb2.Smb2LittleEndianUtils;
import com.mycompany.rocksdb.smb2.Smb2Utils;

import io.vertx.reactivex.core.buffer.Buffer;

// SMB2 SESSION_SETUP Response Structure (section 2.2.6)
public class SessionSetupResponse {
    public static final int STRUCTURE_SIZE = 16;

    private short structureSize; // 2 bytes
    private short sessionFlags; // 2 bytes
    private int securityBufferOffset; // 2 bytes (offset from start of response)
    private int securityBufferLength; // 2 bytes
    private long reserved; // 8 bytes

    private byte[] securityBuffer; // Variable length (GSSAPI/SPNEGO token)

    public SessionSetupResponse() {
        this.structureSize = STRUCTURE_SIZE;
        this.securityBuffer = new byte[0];
    }

    public Buffer encode() {
        Buffer buffer = Buffer.buffer();

        Smb2Utils.writeShortLE(buffer, structureSize);
        Smb2Utils.writeShortLE(buffer, sessionFlags);
        // Calculate offsets based on current buffer length
        this.securityBufferOffset = Smb2Header.STRUCTURE_SIZE + 8; // Header + Session Setup Response fixed part
        Smb2Utils.writeShortLE(buffer, (short) securityBufferOffset);
        Smb2Utils.writeShortLE(buffer, (short) securityBuffer.length);
        if (securityBuffer != null) {
            Smb2Utils.writeBytes(buffer, securityBuffer);
        }
        return buffer;
    }

    // Getters and Setters
    public short getStructureSize() {
        return structureSize;
    }

    public void setStructureSize(short structureSize) {
        this.structureSize = structureSize;
    }

    public short getSessionFlags() {
        return sessionFlags;
    }

    public void setSessionFlags(short sessionFlags) {
        this.sessionFlags = sessionFlags;
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

    public long getReserved() {
        return reserved;
    }

    public void setReserved(long reserved) {
        this.reserved = reserved;
    }

    public byte[] getSecurityBuffer() {
        return securityBuffer;
    }

    public void setSecurityBuffer(byte[] securityBuffer) {
        this.securityBuffer = securityBuffer;
    }
}

