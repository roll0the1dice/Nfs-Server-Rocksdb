package com.mycompany.rocksdb.smb2.POJO;

import io.vertx.reactivex.core.buffer.Buffer;
import lombok.Data;

@Data
public class GetInfoResponse {
    private short structureSize;
    private short outputBufferOffset;
    private int outputBufferLength;
    private int reserved;
    private byte[] dataPayload;

    public static final int STRUCTURE_SIZE = 16;

    public GetInfoResponse() {
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

    public int getReserved() {
        return reserved;
    }

    public void setReserved(int reserved) {
        this.reserved = reserved;
    }

    public Buffer encode() {
        Buffer buffer = Buffer.buffer(STRUCTURE_SIZE);
        buffer.appendShortLE(structureSize);
        buffer.appendShortLE(outputBufferOffset);
        buffer.appendIntLE(outputBufferLength);
        // buffer.appendByte((byte) 0); // Align pad (1 byte)
        buffer.appendBytes(dataPayload != null ? dataPayload : new byte[0]);
        return buffer;
    }

    public static GetInfoResponse decode(Buffer buffer, int offset) {
        GetInfoResponse response = new GetInfoResponse();
        response.structureSize = buffer.getShortLE(offset);
        response.outputBufferOffset = buffer.getShortLE(offset + 2);
        response.outputBufferLength = buffer.getIntLE(offset + 4);
        response.reserved = buffer.getIntLE(offset + 8);
        return response;
    }
}

