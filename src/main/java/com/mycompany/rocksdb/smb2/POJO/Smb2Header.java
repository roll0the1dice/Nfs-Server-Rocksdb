package com.mycompany.rocksdb.smb2.POJO;

import com.mycompany.rocksdb.smb2.Smb2Constants;
import com.mycompany.rocksdb.smb2.Smb2Utils;

import io.vertx.reactivex.core.buffer.Buffer;

// SMB2 Header Structure (64 bytes)
public class Smb2Header {
    public static final int STRUCTURE_SIZE = 64;
    public static final int SMB2_HEADER_SIZE = 64;

    private short headerLength; // 2 bytes
    private short creditCharge; // 2 bytes
    private long status; // 4 bytes (NTSTATUS)
    private short command; // 2 bytes
    private short creditRequestResponse; // 2 bytes
    private int flags; // 4 bytes
    private int nextCommand; // 4 bytes (offset to next command if compounded)
    private long messageId; // 8 bytes
    private long reserved; // 4 bytes
    private long treeId; // 4 bytes
    private long sessionId; // 8 bytes
    private byte[] signature; // 16 bytes

    public Smb2Header() {
        this.headerLength = STRUCTURE_SIZE;
        this.creditCharge = 1;
        this.status = Smb2Constants.STATUS_SUCCESS;
        this.signature = new byte[16]; // Initialize with zeros
    }

    // Method to decode SMB2 Header from buffer
    public static Smb2Header decode(Buffer buffer, int currentReqPosition) {
        Smb2Header header = new Smb2Header();
        int offset = currentReqPosition;

        long protocolId = Smb2Utils.readUnsignedInt(buffer, offset); 
        // 0x00000000FE534D42; Protocol ID (magic number)
       
        offset += 4;
        // We expect the protocol ID to be 0xFE534D42
        if (protocolId != Smb2Constants.SMB2_PROTOCOL_ID) {
            throw new IllegalArgumentException("Invalid SMB2 Protocol ID: " + Long.toHexString(protocolId));
        }

        header.setHeaderLength((short) Smb2Utils.readShortLE(buffer, offset));
        offset += 2;
        header.setCreditCharge((short) Smb2Utils.readShortLE(buffer, offset));
        offset += 2;
        header.setStatus(Smb2Utils.readUnsignedInt(buffer, offset)); // NTSTATUS is 4 bytes
        offset += 4;
        header.setCommand((short) Smb2Utils.readShortLE(buffer, offset));
        offset += 2;
        header.setCreditRequestResponse((short) Smb2Utils.readShortLE(buffer, offset));
        offset += 2;
        header.setFlags(Smb2Utils.readIntLE(buffer, offset));
        offset += 4;
        header.setNextCommand(Smb2Utils.readIntLE(buffer, offset));
        offset += 4;
        header.setMessageId(Smb2Utils.readLongLE(buffer, offset));
        offset += 8;
        // header.setReserved(Smb2Utils.readInt(buffer, offset));
        // offset += 4;
        // header.setTreeId(Smb2Utils.readInt(buffer, offset));
        // offset += 4;
        // 3. 处理同步/异步差异
        if ((header.getFlags() & 0x02) != 0) { // SMB2_FLAGS_ASYNC_COMMAND
            // 异步模式：读取 8 字节 AsyncId
            long asyncId = Smb2Utils.readLongLE(buffer, offset);
            // header.setAsyncId(asyncId); 
            offset += 8;
        } else {
            // 同步模式
            header.setReserved(Smb2Utils.readIntLE(buffer, offset)); // 其实是 Reserved
            offset += 4;
            header.setTreeId(Smb2Utils.readIntLE(buffer, offset));
            offset += 4;
        }

        header.setSessionId(Smb2Utils.readLongLE(buffer, offset));
        offset += 8;
        header.setSignature(Smb2Utils.readBytes(buffer, offset, 16));
        offset += 16;

        return header;
    }

    // Method to encode SMB2 Header to buffer
    public Buffer encode() {
        Buffer buffer = Buffer.buffer(64);

        // Write Protocol ID (Magic Number)
        Smb2Utils.writeInt(buffer, Smb2Constants.SMB2_PROTOCOL_ID); // It's 4 bytes, write it as int

        Smb2Utils.writeShortLE(buffer, (short) 64); // Header Length
        Smb2Utils.writeShortLE(buffer, creditCharge);
        Smb2Utils.writeIntLE(buffer, (int)status); // NTSTATUS is 4 bytes
        Smb2Utils.writeShortLE(buffer, command);
        Smb2Utils.writeShortLE(buffer, creditRequestResponse);
        Smb2Utils.writeIntLE(buffer, flags);
        Smb2Utils.writeIntLE(buffer, (int)nextCommand);
        Smb2Utils.writeLongLE(buffer, messageId);
        Smb2Utils.writeIntLE(buffer, (int)reserved);
        Smb2Utils.writeIntLE(buffer, (int)treeId);
        Smb2Utils.writeLongLE(buffer, sessionId);
        Smb2Utils.writeBytes(buffer, signature);

        return buffer;
    }

    // Getters and Setters
    public short getHeaderLength() {
        return headerLength;
    }

    public void setHeaderLength(short headerLength) {
        this.headerLength = headerLength;
    }

    public short getCreditCharge() {
        return creditCharge;
    }

    public void setCreditCharge(short creditCharge) {
        this.creditCharge = creditCharge;
    }

    public long getStatus() {
        return status;
    }

    public void setStatus(long status) {
        this.status = status;
    }

    public short getCommand() {
        return command;
    }

    public void setCommand(short command) {
        this.command = command;
    }

    public short getCreditRequestResponse() {
        return creditRequestResponse;
    }

    public void setCreditRequestResponse(short creditRequestResponse) {
        this.creditRequestResponse = creditRequestResponse;
    }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public int getNextCommand() {
        return nextCommand;
    }

    public void setNextCommand(int nextCommand) {
        this.nextCommand = nextCommand;
    }

    public long getMessageId() {
        return messageId;
    }

    public void setMessageId(long messageId) {
        this.messageId = messageId;
    }

    public long getReserved() {
        return reserved;
    }

    public void setReserved(long reserved) {
        this.reserved = reserved;
    }

    public long getTreeId() {
        return treeId;
    }

    public void setTreeId(long treeId) {
        this.treeId = treeId;
    }

    public long getSessionId() {
        return sessionId;
    }

    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }

    public byte[] getSignature() {
        return signature;
    }

    public void setSignature(byte[] signature) {
        this.signature = signature;
    }
}

