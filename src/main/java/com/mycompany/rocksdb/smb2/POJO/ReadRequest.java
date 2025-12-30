package com.mycompany.rocksdb.smb2.POJO;

import io.vertx.reactivex.core.buffer.Buffer;

public class ReadRequest {
    private short structureSize;
    private byte padding;
    private byte flags;
    private int length;
    private long offset;
    private long fileIdVolatile;
    private long fileIdPersistent;
    private int minimumCount;
    private int channel;
    private int remainingBytes;
    private short readChannelInfoOffset;
    private short readChannelInfoLength;
    private int reserved;
    private byte bufferFiller;

    public static final int STRUCTURE_SIZE = 49;

    public ReadRequest() {
        this.structureSize = STRUCTURE_SIZE;
    }

    public short getStructureSize() {
        return structureSize;
    }

    public byte getPadding() {
        return padding;
    }

    public byte getFlags() {
        return flags;
    }

    public int getLength() {
        return length;
    }

    public long getOffset() {
        return offset;
    }

    public long getFileIdVolatile() {
        return fileIdVolatile;
    }

    public long getFileIdPersistent() {
        return fileIdPersistent;
    }

    public int getMinimumCount() {
        return minimumCount;
    }

    public int getChannel() {
        return channel;
    }

    public int getRemainingBytes() {
        return remainingBytes;
    }

    public short getReadChannelInfoOffset() {
        return readChannelInfoOffset;
    }

    public short getReadChannelInfoLength() {
        return readChannelInfoLength;
    }

    public int getReserved() {
        return reserved;
    }

    public static ReadRequest decode(Buffer buffer, int offset) {
        ReadRequest request = new ReadRequest();
        
        // 1. StructureSize (2 bytes): 0-1, 必须为 49 (0x0031)
        request.structureSize = buffer.getShortLE(offset);
        
        // 2. Padding (1 byte): 2
        request.padding = buffer.getByte(offset + 2);
        
        // 3. Flags (1 byte): 3
        request.flags = buffer.getByte(offset + 3);
        
        // 4. Length (4 bytes): 4-7
        request.length = buffer.getIntLE(offset + 4);
        
        // 5. Offset (8 bytes): 8-15
        request.offset = buffer.getLongLE(offset + 8);
        
        // 6. FileId (16 bytes): 16-31
        // 注意：SMB2 GUID Handle 由 8字节 Volatile 和 8字节 Persistent 组成
        request.fileIdPersistent = buffer.getLongLE(offset + 16);
        request.fileIdVolatile = buffer.getLongLE(offset + 24);
        
        // 7. MinimumCount (4 bytes): 32-35
        request.minimumCount = buffer.getIntLE(offset + 32);
        
        // 8. Channel (4 bytes): 36-39
        request.channel = buffer.getIntLE(offset + 36);
        
        // 9. RemainingBytes (4 bytes): 40-43
        request.remainingBytes = buffer.getIntLE(offset + 40);
        
        // 10. ReadChannelInfoOffset (2 bytes): 44-45
        request.readChannelInfoOffset = buffer.getShortLE(offset + 44);
        
        // 11. ReadChannelInfoLength (2 bytes): 46-47
        request.readChannelInfoLength = buffer.getShortLE(offset + 46);
        
        // 12. Buffer (1 byte): 48
        // 协议规定请求体最后一个字节是 Buffer[1]，通常作为占位符或填充。
        // 你的原代码在此处 getIntLE(offset + 48) 是错误的，因为它读了 4 个字节。
        request.bufferFiller = buffer.getByte(offset + 48); 

        return request;
    }

    public Buffer encode() {
        Buffer buffer = Buffer.buffer(STRUCTURE_SIZE);
        buffer.appendShortLE(structureSize);
        buffer.appendByte(padding);
        buffer.appendByte(flags);
        buffer.appendIntLE(length);
        buffer.appendLongLE(offset);
        buffer.appendLongLE(fileIdVolatile);
        buffer.appendLongLE(fileIdPersistent);
        buffer.appendIntLE(minimumCount);
        buffer.appendIntLE(channel);
        buffer.appendIntLE(remainingBytes);
        buffer.appendShortLE(readChannelInfoOffset);
        buffer.appendShortLE(readChannelInfoLength);
        buffer.appendIntLE(reserved); // This is 1 byte reserved + 3 bytes padding in spec, simplify to 4 bytes reserved
        return buffer;
    }
}

