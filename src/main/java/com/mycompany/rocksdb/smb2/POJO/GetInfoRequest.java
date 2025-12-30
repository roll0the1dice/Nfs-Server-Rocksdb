package com.mycompany.rocksdb.smb2.POJO;

import io.vertx.reactivex.core.buffer.Buffer;

public class GetInfoRequest {
    private short structureSize;
    private byte infoType;
    private byte fileInfoClass;
    private int bufferHint;
    private int flags;
    private long fileIdVolatile;
    private long fileIdPersistent;
    private int outputBufferLength;
    private short inputBufferOffset;
    private short inputBufferLength;
    private int additionalInformation;

    public static final int STRUCTURE_SIZE = 32;

    public GetInfoRequest() {
        this.structureSize = STRUCTURE_SIZE;
    }

    public short getStructureSize() {
        return structureSize;
    }

    public byte getInfoType() {
        return infoType;
    }

    public byte getFileInfoClass() {
        return fileInfoClass;
    }

    public int getBufferHint() {
        return bufferHint;
    }

    public int getFlags() {
        return flags;
    }

    public long getFileIdVolatile() {
        return fileIdVolatile;
    }

    public long getFileIdPersistent() {
        return fileIdPersistent;
    }

    public int getOutputBufferLength() {
        return outputBufferLength;
    }

    public short getInputBufferOffset() {
        return inputBufferOffset;
    }

    public short getInputBufferLength() {
        return inputBufferLength;
    }

    public static GetInfoRequest decode(Buffer buffer, int startOffset) {
        GetInfoRequest request = new GetInfoRequest();
        int index = startOffset; // 使用 index 作为游标

        // 1. StructureSize (2 bytes) - 预期 0x0029
        request.structureSize = buffer.getShortLE(index);
        index += 2;

        // 2. InfoType (1 byte)
        request.infoType = buffer.getByte(index);
        index += 1;

        // 3. FileInfoClass (1 byte)
        request.fileInfoClass = buffer.getByte(index);
        index += 1;

        // 4. OutputBufferLength (4 bytes) - 对应截图中的 Max Response Size
        request.outputBufferLength = buffer.getIntLE(index);
        index += 4;

        // 5. InputBufferOffset (2 bytes)
        request.inputBufferOffset = buffer.getShortLE(index);
        index += 2;

        // 6. Reserved (2 bytes) - 跳过保留字段
        index += 2;

        // 7. InputBufferLength (4 bytes)
        request.inputBufferLength = (short)buffer.getIntLE(index);
        index += 4;

        // 8. AdditionalInformation (4 bytes)
        request.additionalInformation = buffer.getIntLE(index);
        index += 4;

        // 9. Flags (4 bytes)
        request.flags = buffer.getIntLE(index);
        index += 4;

        // 10. FileId (16 bytes) - 包含 Persistent 和 Volatile
        // 规范要求：Persistent ID 在前，Volatile ID 在后
        request.fileIdPersistent = buffer.getLongLE(index);
        index += 8;

        request.fileIdVolatile = buffer.getLongLE(index);
        index += 8;

        return request;
    }

    public Buffer encode() {
        Buffer buffer = Buffer.buffer(STRUCTURE_SIZE);
        buffer.appendShortLE(structureSize);
        buffer.appendByte(infoType);
        buffer.appendByte(fileInfoClass);
        buffer.appendIntLE(bufferHint);
        buffer.appendIntLE(flags);
        buffer.appendLongLE(fileIdVolatile);
        buffer.appendLongLE(fileIdPersistent);
        buffer.appendIntLE(outputBufferLength);
        buffer.appendShortLE(inputBufferOffset);
        buffer.appendShortLE(inputBufferLength);
        return buffer;
    }
}

