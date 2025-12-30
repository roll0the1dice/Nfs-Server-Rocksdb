package com.mycompany.rocksdb.smb2.POJO;

import io.vertx.reactivex.core.buffer.Buffer;
import lombok.Data;

@Data
public class QueryDirectoryResponse {
    private short structureSize;
    private short outputBufferOffset;
    private int outputBufferLength;
    private byte[] bufferData;

    public static final int STRUCTURE_SIZE = 8; // Excluding data

    public QueryDirectoryResponse() {
        this.structureSize = STRUCTURE_SIZE;
    }

    public short getStructureSize() {
        return structureSize;
    }

    public void setStructureSize(short structureSize) {
        this.structureSize = structureSize;
    }

    public short getOutputBufferOffset() {
        return outputBufferOffset;
    }

    public void setOutputBufferOffset(short outputBufferOffset) {
        this.outputBufferOffset = outputBufferOffset;
    }

    public int getOutputBufferLength() {
        return outputBufferLength;
    }

    public void setOutputBufferLength(int outputBufferLength) {
        this.outputBufferLength = outputBufferLength;
    }

    public Buffer encode() {
        Buffer buffer = Buffer.buffer(STRUCTURE_SIZE);
        buffer.appendShortLE(structureSize);
        buffer.appendShortLE(outputBufferOffset);
        buffer.appendIntLE(outputBufferLength);
        buffer.appendBytes(bufferData);
        return buffer;
    }

    public static QueryDirectoryResponse decode(Buffer buffer, int offset) {
        QueryDirectoryResponse response = new QueryDirectoryResponse();
        response.structureSize = buffer.getShortLE(offset);
        response.outputBufferOffset = buffer.getShortLE(offset + 2);
        response.outputBufferLength = buffer.getIntLE(offset + 4);
        return response;
    }
}

