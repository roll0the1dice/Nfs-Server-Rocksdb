package com.mycompany.rocksdb.smb2.POJO;

import com.mycompany.rocksdb.smb2.Smb2LittleEndianUtils;
import com.mycompany.rocksdb.smb2.Smb2Utils;

import io.vertx.reactivex.core.buffer.Buffer;


// SMB2 SESSION_SETUP Request Structure (section 2.2.5)
public class SessionSetupRequest {
    public static final int STRUCTURE_SIZE = 24;

    private short structureSize; // 2 bytes
    private short flags; // 1 byte
    private byte securityMode; // 1 byte
    private long capabilities; // 4 bytes
    private long channel; // 4 bytes
    private int securityBufferOffset; // 2 bytes
    private int securityBufferLength; // 2 bytes
    private long previousSessionId; // 8 bytes

    private byte[] securityBuffer; // Variable length

    public SessionSetupRequest() {
        this.structureSize = STRUCTURE_SIZE;
        this.securityBuffer = new byte[0];
    }

    /**
     * @param buffer 原始字节流
     * @param headerOffset SMB2 Header 的起始位置 (极其重要)
     * @param bodyOffset SessionSetupRequest 结构的起始位置 (通常是 headerOffset + 64)
     */
    public static SessionSetupRequest decode(Buffer buffer, int bodyOffset) {
        SessionSetupRequest request = new SessionSetupRequest();
        int current = bodyOffset;

        // 1. 读取固定长度的结构字段
        request.setStructureSize((short) Smb2Utils.readShortLE(buffer, current));
        current += 2;
        
        request.setFlags((byte) buffer.getByte(current)); 
        current += 1;
        
        request.setSecurityMode(buffer.getByte(current)); 
        current += 1;
        
        request.setCapabilities(Smb2Utils.readIntLE(buffer, current));
        current += 4;
        
        request.setChannel(Smb2Utils.readIntLE(buffer, current)); 
        current += 4;
        
        // 关键点：这两个偏移量和长度是从 Header 起始处算的
        int securityBufferOffset = Smb2Utils.readShortLE(buffer, current);
        request.setSecurityBufferOffset((short) securityBufferOffset);
        current += 2;
        
        int securityBufferLength = Smb2Utils.readShortLE(buffer, current);
        request.setSecurityBufferLength((short) securityBufferLength);
        current += 2;
        
        request.setPreviousSessionId(Smb2Utils.readLongLE(buffer, current));
        current += 8;

        // 2. 读取 Security Buffer (Blob)
        // 错误修正：实际位置 = Header 起始位置 + 协议给出的偏移量
        int securityBufferActualOffset = securityBufferOffset;

        // 3. 安全性检查并提取字节
        if (securityBufferLength > 0) {
            // 确保不会越界
            if (securityBufferActualOffset + securityBufferLength <= buffer.length()) {
                request.setSecurityBuffer(Smb2Utils.readBytes(buffer, securityBufferActualOffset, securityBufferLength));
            } else {
                // 异常处理：数据包长度不足
                throw new RuntimeException("Security Buffer exceeds buffer length");
            }
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

    public short getFlags() {
        return flags;
    }

    public void setFlags(short flags) {
        this.flags = flags;
    }

    public byte getSecurityMode() {
        return securityMode;
    }

    public void setSecurityMode(byte securityMode) {
        this.securityMode = securityMode;
    }

    public long getCapabilities() {
        return capabilities;
    }

    public void setCapabilities(long capabilities) {
        this.capabilities = capabilities;
    }

    public long getChannel() {
        return channel;
    }

    public void setChannel(long channel) {
        this.channel = channel;
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

    public long getPreviousSessionId() {
        return previousSessionId;
    }

    public void setPreviousSessionId(long previousSessionId) {
        this.previousSessionId = previousSessionId;
    }

    public byte[] getSecurityBuffer() {
        return securityBuffer;
    }

    public void setSecurityBuffer(byte[] securityBuffer) {
        this.securityBuffer = securityBuffer;
    }
}

