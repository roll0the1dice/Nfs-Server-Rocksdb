package com.mycompany.rocksdb.smb2.POJO;

import com.mycompany.rocksdb.smb2.Smb2Constants;
import com.mycompany.rocksdb.smb2.Smb2LittleEndianUtils;
import com.mycompany.rocksdb.smb2.Smb2Utils;

import io.vertx.reactivex.core.buffer.Buffer;

import java.util.ArrayList;
import java.util.List;

// SMB2 NEGOTIATE Request Structure (section 2.2.3)
public class NegotiateRequest {
    public static final int STRUCTURE_SIZE = 36;

    private short structureSize; // 2 bytes
    private short dialectCount; // 2 bytes
    private short securityMode; // 2 bytes
    private short reserved; // 2 bytes
    private long capabilities; // 4 bytes
    private byte[] clientGuid; // 16 bytes
    private long clientStartTime; // 8 bytes (FILETIME)
    private List<Short> dialects; // variable length

    public NegotiateRequest() {
        this.structureSize = STRUCTURE_SIZE;
        this.dialects = new ArrayList<>();
        this.clientGuid = new byte[16];
    }

    public static NegotiateRequest decode(Buffer buffer, int offset) {
        NegotiateRequest request = new NegotiateRequest();

        request.setStructureSize((short) Smb2Utils.readShortLE(buffer, offset));
        if (request.getStructureSize() != 0x24) {
            throw new IllegalArgumentException("Invalid Negotiate Request StructureSize: " + request.getStructureSize());
        }
        offset += 2;
        // 2. 读取协议数量
        request.setDialectCount((short) Smb2Utils.readShortLE(buffer, offset));
        offset += 2;
        // 3. 安全模式
        request.setSecurityMode((short) Smb2Utils.readShortLE(buffer, offset));
        offset += 2;
        // 4. 跳过 Reserved (2字节)
        request.setReserved((short) Smb2Utils.readShortLE(buffer, offset));
        offset += 2;
         // 5. 读取能力
        request.setCapabilities(Smb2Utils.readIntLE(buffer, offset));
        offset += 4;
        // 6. 读取 Client Guid (16 字节)
        request.setClientGuid(Smb2Utils.readBytes(buffer, offset, 16));
        offset += 16;
        // 7. 读取 NegotiateContext 相关 (SMB 3.x 关键)
        int contextOffset = buffer.getIntLE(offset);
        offset += 4;
        int contextCount = buffer.getShortLE(offset);
        offset += 2;

        // 跳过 Reserved2
        offset += 2;

        // 8. **核心：循环读取所有 Dialects**
        int dialectCount = request.getDialectCount();
        for (int i = 0; i < dialectCount; i++) {
            short dialect = buffer.getShortLE(offset);
            request.addDialect(dialect);
            offset += 2;
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

    public short getDialectCount() {
        return dialectCount;
    }

    public void setDialectCount(short dialectCount) {
        this.dialectCount = dialectCount;
    }

    public short getSecurityMode() {
        return securityMode;
    }

    public void setSecurityMode(short securityMode) {
        this.securityMode = securityMode;
    }

    public short getReserved() {
        return reserved;
    }

    public void setReserved(short reserved) {
        this.reserved = reserved;
    }

    public long getCapabilities() {
        return capabilities;
    }

    public void setCapabilities(long capabilities) {
        this.capabilities = capabilities;
    }

    public byte[] getClientGuid() {
        return clientGuid;
    }

    public void setClientGuid(byte[] clientGuid) {
        this.clientGuid = clientGuid;
    }

    public long getClientStartTime() {
        return clientStartTime;
    }

    public void setClientStartTime(long clientStartTime) {
        this.clientStartTime = clientStartTime;
    }

    public List<Short> getDialects() {
        return dialects;
    }

    public void addDialect(short dialect) {
        this.dialects.add(dialect);
    }

}

