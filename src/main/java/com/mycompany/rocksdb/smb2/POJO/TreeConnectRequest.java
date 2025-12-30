package com.mycompany.rocksdb.smb2.POJO;

import com.mycompany.rocksdb.smb2.Smb2LittleEndianUtils;
import com.mycompany.rocksdb.smb2.Smb2Utils;

import io.vertx.reactivex.core.buffer.Buffer;

// SMB2 TREE_CONNECT Request Structure (section 2.2.9)
public class TreeConnectRequest {
    public static final int STRUCTURE_SIZE = 9;

    private short structureSize; // 2 bytes
    private short flags; // 2 bytes
    private int pathOffset; // 2 bytes (offset from start of SMB2 header)
    private int pathLength; // 2 bytes
    private byte[] reserved; // 1 byte

    private String path; // Variable length UTF-16LE string

    public TreeConnectRequest() {
        this.structureSize = STRUCTURE_SIZE;
        this.reserved = new byte[1];
    }

    public static TreeConnectRequest decode(Buffer buffer, int bodyOffset) {
        TreeConnectRequest request = new TreeConnectRequest();
        int current = bodyOffset;

        // 1. 读取固定字段（SMB2 必须使用小端序 LE）
        // StructureSize (2字节) - 截图显示 0x0009
        request.setStructureSize((short) Smb2Utils.readShortLE(buffer, current));
        current += 2;

        // Flags (2字节) - 截图显示 0x0000
        request.setFlags((short) Smb2Utils.readShortLE(buffer, current));
        current += 2;

        // PathOffset (2字节) - 截图显示 0x0048 (72)
        int pathOffset = Smb2Utils.readShortLE(buffer, current);
        request.setPathOffset((short) pathOffset);
        current += 2;

        // PathLength (2字节) - 截图显示 32 (对应 "\\127.0.0.1\IPC$" 的 UTF-16 长度)
        int pathLength = Smb2Utils.readShortLE(buffer, current);
        request.setPathLength((short) pathLength);
        current += 2;

        // 注意：不要手动增加 offset 来读所谓“Reserved”字节！
        // 虽然 StructureSize 是 9，但在 TreeConnectRequest 中，前 8 字节就是固定字段，
        // 第 9 个字节其实就是路径字符串（Path）的起始位置（如果 Offset 指向这里的话）。

        // 2. 计算路径字符串的实际位置
        // 关键点：协议规定 PathOffset 是相对于 SMB2 Header 起始位置的偏移
        int actualPathOffset = pathOffset;

        // 3. 提取路径字符串
        if (pathLength > 0 && actualPathOffset + pathLength <= buffer.length()) {
            // SMB2 的路径名通常使用 UTF-16LE 编码
            request.setPath(Smb2Utils.readSmb2String(buffer, actualPathOffset, pathLength));
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

    public int getPathOffset() {
        return pathOffset;
    }

    public void setPathOffset(int pathOffset) {
        this.pathOffset = pathOffset;
    }

    public int getPathLength() {
        return pathLength;
    }

    public void setPathLength(int pathLength) {
        this.pathLength = pathLength;
    }

    public byte[] getReserved() {
        return reserved;
    }

    public void setReserved(byte[] reserved) {
        this.reserved = reserved;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}

