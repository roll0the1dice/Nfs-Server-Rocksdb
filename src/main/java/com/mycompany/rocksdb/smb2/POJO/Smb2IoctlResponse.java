package com.mycompany.rocksdb.smb2.POJO;

import com.mycompany.rocksdb.smb2.Smb2LittleEndianUtils;
import com.mycompany.rocksdb.smb2.Smb2Utils;

import io.vertx.reactivex.core.buffer.Buffer;

public class Smb2IoctlResponse {

    private short structureSize;
    private short reserved;
    private int ctlCode;
    private long fileIdVol;
    private long fileIdEph;
    private int inputOffset;
    private int inputCount;
    private int outputOffset;
    private int outputCount;
    private int flags;
    private byte[] outputBuffer;

    public static final int STRUCTURE_SIZE = 49; // Fixed size of the SMB2 IOCTL response structure

    public Buffer encode() {
        Buffer buffer = Buffer.buffer(STRUCTURE_SIZE);

        Smb2Utils.writeShortLE(buffer, structureSize);
        Smb2Utils.writeShortLE(buffer, reserved);
        Smb2Utils.writeIntLE(buffer, ctlCode);
        Smb2Utils.writeLongLE(buffer, fileIdVol);
        Smb2Utils.writeLongLE(buffer, fileIdEph);
        Smb2Utils.writeIntLE(buffer, inputOffset);
        Smb2Utils.writeIntLE(buffer, inputCount);
        Smb2Utils.writeIntLE(buffer, outputOffset);
        Smb2Utils.writeIntLE(buffer, outputCount);
        Smb2Utils.writeIntLE(buffer, flags);
        // 填充 reserved 2
        Smb2Utils.writeIntLE(buffer, 0x0);

        // Write output buffer if present
        if (outputBuffer != null && outputBuffer.length > 0) {
            buffer.appendBytes(outputBuffer);
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

    public short getReserved() {
        return reserved;
    }

    public void setReserved(short reserved) {
        this.reserved = reserved;
    }

    public int getCtlCode() {
        return ctlCode;
    }

    public void setCtlCode(int ctlCode) {
        this.ctlCode = ctlCode;
    }

    public long getFileIdVol() {
        return fileIdVol;
    }

    public void setFileIdVol(long fileIdVol) {
        this.fileIdVol = fileIdVol;
    }

    public long getFileIdEph() {
        return fileIdEph;
    }

    public void setFileIdEph(long fileIdEph) {
        this.fileIdEph = fileIdEph;
    }

    public int getInputOffset() {
        return inputOffset;
    }

    public void setInputOffset(int inputOffset) {
        this.inputOffset = inputOffset;
    }

    public int getInputCount() {
        return inputCount;
    }

    public void setInputCount(int inputCount) {
        this.inputCount = inputCount;
    }

    public int getOutputOffset() {
        return outputOffset;
    }

    public void setOutputOffset(int outputOffset) {
        this.outputOffset = outputOffset;
    }

    public int getOutputCount() {
        return outputCount;
    }

    public void setOutputCount(int outputCount) {
        this.outputCount = outputCount;
    }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public byte[] getOutputBuffer() {
        return outputBuffer;
    }

    public void setOutputBuffer(byte[] outputBuffer) {
        this.outputBuffer = outputBuffer;
    }
}

