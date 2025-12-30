package com.mycompany.rocksdb.smb2.POJO;

import com.mycompany.rocksdb.smb2.Smb2LittleEndianUtils;
import com.mycompany.rocksdb.smb2.Smb2Utils;

import io.vertx.reactivex.core.buffer.Buffer;

public class Smb2IoctlRequest {

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
    private int reserved2;
    private byte[] inputBuffer;
    private byte[] outputBuffer;

    // Constants for the SMB2 IOCTL header
    public static final int STRUCTURE_SIZE = 57; // This is the fixed size of the SMB2 IOCTL request structure
    public static final int IOCTL_HEADER_SIZE = 57; // Size of the fixed part of the IOCTL request without the input buffer
    public static Smb2IoctlRequest decode(Buffer buffer, 
                                        int ioctlStartOffset) {
        Smb2IoctlRequest request = new Smb2IoctlRequest();
        int currentOffset = ioctlStartOffset;

        request.setStructureSize(Smb2Utils.readShortLE(buffer, currentOffset));
        currentOffset += 2;

        request.setReserved(Smb2Utils.readShortLE(buffer, currentOffset));
        currentOffset += 2;

        request.setCtlCode(Smb2Utils.readIntLE(buffer, currentOffset));
        currentOffset += 4;

        request.setFileIdVol(Smb2Utils.readLongLE(buffer, currentOffset));
        currentOffset += 8;
        request.setFileIdEph(Smb2Utils.readLongLE(buffer, currentOffset));
        currentOffset += 8;

        request.setInputOffset(Smb2Utils.readIntLE(buffer, currentOffset));
        currentOffset += 4;
        request.setInputCount(Smb2Utils.readIntLE(buffer, currentOffset));
        currentOffset += 4;

        request.setOutputOffset(Smb2Utils.readIntLE(buffer, currentOffset));
        currentOffset += 4;
        request.setOutputCount(Smb2Utils.readIntLE(buffer, currentOffset));
        currentOffset += 4;

        request.setFlags(Smb2Utils.readIntLE(buffer, currentOffset));
        currentOffset += 4;

        // Reserved2 (4 bytes)
        currentOffset += 4;

        // ====== 正确提取 Input Buffer ======
        if (request.getInputCount() > 0) {
            int inputAbsoluteOffset = request.getInputOffset();

            // 边界检查（Vert.x 用 length()）
            if (inputAbsoluteOffset >= 0 && 
                inputAbsoluteOffset + request.getInputCount() <= buffer.length()) {
                
                // 使用 getByteBuf() 或 slice() 提取子缓冲区
                request.setInputBuffer(buffer.getBytes(inputAbsoluteOffset, 
                                                    inputAbsoluteOffset + request.getInputCount()));
            } else {
                throw new IllegalArgumentException("Invalid InputOffset/InputCount: out of bounds");
            }
        } else {
            request.setInputBuffer(new byte[0]);
        }

        // Output Buffer（请求中通常为0）
        if (request.getOutputCount() > 0) {
            int outputAbsoluteOffset = request.getOutputOffset();
            if (outputAbsoluteOffset >= 0 && 
                outputAbsoluteOffset + request.getOutputCount() <= buffer.length()) {
                
                request.setOutputBuffer(buffer.getBytes(outputAbsoluteOffset, 
                                                    outputAbsoluteOffset + request.getOutputCount()));
            }
        } else {
            request.setOutputBuffer(new byte[0]);
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

    public byte[] getInputBuffer() {
        return inputBuffer;
    }

    public void setInputBuffer(byte[] inputBuffer) {
        this.inputBuffer = inputBuffer;
    }

    public byte[] getOutputBuffer() {
        return outputBuffer;
    }

    public void setOutputBuffer(byte[] outputBuffer) {
        this.outputBuffer = outputBuffer;
    }

    // TODO: Add an encode method if this POJO needs to be serialized for client-side IOCTL requests
}

