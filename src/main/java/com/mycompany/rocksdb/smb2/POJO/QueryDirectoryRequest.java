package com.mycompany.rocksdb.smb2.POJO;

import io.vertx.reactivex.core.buffer.Buffer;

public class QueryDirectoryRequest {
    private short structureSize;
    private byte fileInformationClass;
    private byte flags;
    private int fileIndex;
    private long fileIdVolatile;
    private long fileIdPersistent;
    private short fileNameOffset;
    private short fileNameLength;
    private int outputBufferLength;
    private String searchPattern;

    public static final int STRUCTURE_SIZE = 32; // Excluding search pattern

    public QueryDirectoryRequest() {
        this.structureSize = STRUCTURE_SIZE;
    }

    public short getStructureSize() {
        return structureSize;
    }

    public byte getFileInformationClass() {
        return fileInformationClass;
    }

    public byte getFlags() {
        return flags;
    }

    public int getFileIndex() {
        return fileIndex;
    }

    public long getFileIdVolatile() {
        return fileIdVolatile;
    }

    public long getFileIdPersistent() {
        return fileIdPersistent;
    }

    public short getFileNameOffset() {
        return fileNameOffset;
    }

    public short getFileNameLength() {
        return fileNameLength;
    }

    public int getOutputBufferLength() {
        return outputBufferLength;
    }

    public String getSearchPattern() {
        return searchPattern;
    }

    public static QueryDirectoryRequest decode(Buffer buffer, int bodyOffset) {
        QueryDirectoryRequest request = new QueryDirectoryRequest();
        int pos = bodyOffset;

        // 1. StructureSize (2 bytes) - 必须为 0x0021
        request.structureSize = buffer.getShortLE(pos);
        pos += 2;

        // 2. FileInformationClass (1 byte)
        request.fileInformationClass = buffer.getByte(pos);
        pos += 1;

        // 3. Flags (1 byte)
        request.flags = buffer.getByte(pos);
        pos += 1;

        // 4. FileIndex (4 bytes)
        request.fileIndex = buffer.getIntLE(pos);
        pos += 4;

        // 5. FileId (16 bytes: 8 Persistent + 8 Volatile)
        // 注意：Persistent 在前，Volatile 在后
        request.fileIdPersistent = buffer.getLongLE(pos);
        pos += 8;
        request.fileIdVolatile = buffer.getLongLE(pos);
        pos += 8;

        // 6. FileNameOffset (2 bytes)
        request.fileNameOffset = buffer.getShortLE(pos);
        pos += 2;

        // 7. FileNameLength (2 bytes)
        request.fileNameLength = buffer.getShortLE(pos);
        pos += 2;

        // 8. OutputBufferLength (4 bytes)
        request.outputBufferLength = buffer.getIntLE(pos);
        pos += 4;

        // --- 解析 SearchPattern (文件名/匹配模式) ---
        if (request.fileNameLength > 0 && request.fileNameOffset > 0) {
            // 重要：根据 [MS-SMB2]，Offset 是从 SMB2 Header 开始计算的偏移
            int absoluteFileNameOffset = bodyOffset + request.fileNameOffset;
            
            // 确保读取范围在 Buffer 有效范围内
            if (absoluteFileNameOffset + request.fileNameLength <= buffer.length()) {
                request.searchPattern = buffer.getString(
                    absoluteFileNameOffset, 
                    absoluteFileNameOffset + request.fileNameLength, 
                    "UTF-16LE"
                );
            }
        }

        return request;
    }

    public Buffer encode() {
        Buffer buffer = Buffer.buffer(STRUCTURE_SIZE);
        buffer.appendShortLE(structureSize);
        buffer.appendByte(fileInformationClass);
        buffer.appendByte(flags);
        buffer.appendIntLE(fileIndex);
        buffer.appendIntLE(0); // Reserved
        buffer.appendLongLE(fileIdVolatile);
        buffer.appendLongLE(fileIdPersistent);
        buffer.appendShortLE(fileNameOffset);
        buffer.appendShortLE(fileNameLength);
        buffer.appendIntLE(outputBufferLength);
        return buffer;
    }
}

