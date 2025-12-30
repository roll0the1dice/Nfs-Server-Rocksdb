package com.mycompany.rocksdb.smb2.POJO;

import io.vertx.reactivex.core.buffer.Buffer;

public class ReadResponse {
    private short structureSize;
    private byte dataOffset;
    private byte reserved;
    private int dataLength;
    private int dataRemaining;
    private int reserved2;
    private Buffer dataBuffer; // The actual data read from the file

    public static final int STRUCTURE_SIZE = 17; // Excluding dataBuffer
    public static final int responseHeaderSize = 16;

    public ReadResponse() {
        this.structureSize = STRUCTURE_SIZE;
    }

    public short getStructureSize() {
        return structureSize;
    }

    public void setStructureSize(short structureSize) {
        this.structureSize = structureSize;
    }

    public byte getDataOffset() {
        return dataOffset;
    }

    public void setDataOffset(byte dataOffset) {
        this.dataOffset = dataOffset;
    }

    public byte getReserved() {
        return reserved;
    }

    public void setReserved(byte reserved) {
        this.reserved = reserved;
    }

    public int getDataLength() {
        return dataLength;
    }

    public void setDataLength(int dataLength) {
        this.dataLength = dataLength;
    }

    public int getDataRemaining() {
        return dataRemaining;
    }

    public void setDataRemaining(int dataRemaining) {
        this.dataRemaining = dataRemaining;
    }

    public int getReserved2() {
        return reserved2;
    }

    public void setReserved2(int reserved2) {
        this.reserved2 = reserved2;
    }

    public Buffer getDataBuffer() {
        return dataBuffer;
    }

    public void setDataBuffer(Buffer dataBuffer) {
        this.dataBuffer = dataBuffer;
    }

    public Buffer encode() {
        // 1. 计算总长度：响应头固定 16 字节 + 实际数据的长度
        int responseHeaderLength = 16; 
        int totalLength = responseHeaderLength + (dataBuffer != null ? dataBuffer.length() : 0);
        
        // 2. 分配足够的 Buffer 空间
        Buffer buffer = Buffer.buffer(totalLength);
        
        // 3. 写入 StructureSize
        // 必须是 17 (0x11)，即使这个头结构体本身只占 16 字节
        buffer.appendShortLE((short) 17); 
        
        // 4. 写入 DataOffset
        // 它是从 SMB2 Header 的起始位置到 Data 的偏移量。
        // 通常是 64 (Header) + 16 (Response Struct) = 80 (0x50)
        buffer.appendByte(dataOffset); 
        
        // 5. 写入 Reserved (1 byte)
        buffer.appendByte((byte) 0); 
        
        // 6. 写入 DataLength (4 bytes)
        buffer.appendIntLE(dataLength);
        
        // 7. 写入 DataRemaining (4 bytes)
        buffer.appendIntLE(dataRemaining);
        
        // 8. 写入 Reserved2 (4 bytes)
        buffer.appendIntLE(0);  
        
        // 9. 【关键修正】追加实际的数据内容
        if (dataBuffer != null) {
            buffer.appendBuffer(dataBuffer);
        }
        
        return buffer;
    }

    public static ReadResponse decode(Buffer buffer, int offset) {
        ReadResponse response = new ReadResponse();
        response.structureSize = buffer.getShortLE(offset);
        response.dataOffset = buffer.getByte(offset + 2);
        response.reserved = buffer.getByte(offset + 3);
        response.dataLength = buffer.getIntLE(offset + 4);
        response.dataRemaining = buffer.getIntLE(offset + 8);
        response.reserved2 = buffer.getIntLE(offset + 12);
        // Data buffer will be appended separately after the header
        return response;
    }
}

