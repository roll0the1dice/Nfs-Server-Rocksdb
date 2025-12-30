package com.mycompany.rocksdb.smb2;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.attribute.FileTime;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.UUID;

import com.mycompany.rocksdb.smb2.POJO.Smb2Header;

import io.vertx.reactivex.core.buffer.Buffer;

public class Smb2Utils {

    // Reads a 2-byte unsigned short (little-endian)
    public static int readShort(Buffer buffer, int offset) {
        return buffer.getShort(offset) & 0xFFFF;
    }

    // Writes a 2-byte unsigned short (little-endian)
    public static void writeShort(Buffer buffer, int value) {
        buffer.appendShort((short) value);
    }

    // Reads a 4-byte unsigned int (little-endian)
    public static int readInt(Buffer buffer, int offset) {
        return (int)(buffer.getInt(offset) & 0xFFFFFFFFL);
    }
    
    // Reads a 4-byte unsigned int (little-endian)
    public static long readUnsignedInt(Buffer buffer, int offset) {
        return (buffer.getUnsignedInt(offset) & 0xFFFFFFFFL);
    }

    // Writes a 4-byte unsigned int (little-endian)
    public static void writeInt(Buffer buffer, long value) {
        buffer.appendInt((int) value);
    }

    // Reads an 8-byte unsigned long (little-endian)
    public static long readLong(Buffer buffer, int offset) {
        return buffer.getLong(offset);
    }

    // Writes an 8-byte unsigned long (little-endian)
    public static void writeLong(Buffer buffer, long value) {
        buffer.appendLong(value);
    }

    // Reads a fixed-length byte array
    public static byte[] readBytes(Buffer buffer, int offset, int length) {
        byte[] bytes = new byte[length];
        buffer.getBytes(offset, offset + length, bytes);
        return bytes;
    }

    // Writes a byte array
    public static void writeBytes(Buffer buffer, byte[] bytes) {
        if (bytes != null && bytes.length > 0) {
            buffer.appendBytes(bytes);
        }
    }
    

    // Reads a NULL-terminated UTF-16-LE string
    public static String readSmb2String(Buffer buffer, int offset, int maxLengthBytes) {
        int endOffset = offset;
        while (endOffset < offset + maxLengthBytes && (buffer.getByte(endOffset) != 0 || buffer.getByte(endOffset + 1) != 0)) {
            endOffset += 2;
        }
        byte[] bytes = new byte[endOffset - offset];
        buffer.getBytes(offset, endOffset, bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_16LE);
    }

    // Writes a UTF-16-LE string, optionally NULL-terminating and padding to a 2-byte boundary
    public static void writeSmb2String(Buffer buffer, String value, boolean nullTerminate, int paddingBytes) {
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_16LE);
        buffer.appendBytes(bytes);
        if (nullTerminate) {
            buffer.appendByte((byte)0);
            buffer.appendByte((byte)0);
        }
        // Pad to 2-byte boundary (SMB2 strings are often 2-byte aligned)
        int currentLength = bytes.length + (nullTerminate ? 2 : 0);
        int pad = (paddingBytes - (currentLength % paddingBytes)) % paddingBytes; // Calculate padding needed to reach next multiple of paddingBytes
        for (int i = 0; i < pad; i++) {
            buffer.appendByte((byte) 0);
        }
    }

        // Writes a UTF-16-LE string, optionally NULL-terminating and padding to a 2-byte boundary
    public static void writeSmb2String(Buffer buffer, String value) {
        writeSmb2String(buffer, value, false, 4);
    }

    // Helper to read 16-byte GUID
    public static String readGuid(Buffer buffer, int offset) {
        // GUIDs are typically stored as little-endian for the first 3 fields, then big-endian for the rest.
        // Vert.x buffer reads in network byte order (big-endian) by default for multi-byte types
        // So we need to manually read bytes and reconstruct.
        StringBuilder sb = new StringBuilder();
        // Data1 (4 bytes, little-endian)
        long data1 = (readInt(buffer, offset + 0) & 0xFFFFFFFFL); // Already handled little-endian byte order in readInt.
        sb.append(String.format("%08x-", data1));

        // Data2 (2 bytes, little-endian)
        int data2 = readShort(buffer, offset + 4);
        sb.append(String.format("%04x-", data2));

        // Data3 (2 bytes, little-endian)
        int data3 = readShort(buffer, offset + 6);
        sb.append(String.format("%04x-", data3));

        // Data4 (8 bytes, big-endian)
        for (int i = 0; i < 8; i++) {
            sb.append(String.format("%02x", buffer.getByte(offset + 8 + i)));
            if (i == 1) sb.append("-"); // after 2 bytes, for the 8-byte group
        }
        return sb.toString();
    }

    // Helper to write 16-byte GUID (simplified, actual implementation might need more sophisticated handling)
    public static void writeGuid(Buffer buffer, String guidString) {
        // This is a simplified implementation. A robust solution would parse the GUID string
        // into its components and write them with correct endianness.
        // For now, we'll just write 16 zero bytes as a placeholder.
        for (int i = 0; i < 16; i++) {
            buffer.appendByte((byte) 0);
        }
    }
    
    public static short readShortLE(Buffer buffer, int offset) {
        // Vert.x Buffer's getShortLE reads a 16-bit short in little-endian byte order
        return buffer.getShortLE(offset);
    }

    // Reads a 4-byte unsigned int (little-endian)
    public static int readIntLE(Buffer buffer, int offset) {
        return (int)(buffer.getIntLE(offset) & 0xFFFFFFFFL);
    }

    public static long readLongLE(Buffer buffer, int offset) {
        // Vert.x Buffer's getLongLE reads a 64-bit long in little-endian byte order
        return buffer.getLongLE(offset);
    }

    public static void writeShortLE(Buffer buffer, short value) {
        buffer.appendShortLE(value);
    }

    public static void writeIntLE(Buffer buffer, int value) {
        buffer.appendIntLE(value);
    }

    public static void writeLongLE(Buffer buffer, long value) {
        buffer.appendLongLE(value);
    }

    public static long getPreciseFileTime() {
        Instant now = Instant.now();
        long MILLIS_BETWEEN_1601_AND_1970 = 11644473600000L;
        
        long fileTime = (now.getEpochSecond() * 1000L + MILLIS_BETWEEN_1601_AND_1970) * 10000L;
        fileTime += (now.getNano() / 100L); // 加入纳秒部分（转为100纳秒单位）
        return fileTime;
    }

    public static byte[] generateServerGuid() {
        UUID uuid = UUID.randomUUID();
        ByteBuffer bb = ByteBuffer.allocate(16);
        
        // 按照 SMB2 惯用的混合字节序 (Mixed Endian) 写入 (可选，但更规范)
        // 如果嫌麻烦，直接使用 bb.putLong(msb).putLong(lsb) 也可以，Windows 通常能识别
        bb.order(ByteOrder.LITTLE_ENDIAN);
        
        // 假设将 UUID 映射到 GUID 结构
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        
        // 前 8 字节处理为小端 (Data1, Data2, Data3)
        bb.putInt((int)(msb >>> 32));      // Data1
        bb.putShort((short)(msb >>> 16));  // Data2
        bb.putShort((short)msb);           // Data3
        
        // 后 8 字节保持大端 (Data4)
        bb.order(ByteOrder.BIG_ENDIAN);
        bb.putLong(lsb);                   // Data4
        
        return bb.array();
    }

    public static long generateServerGuidLow() {
        // 假设将 UUID 映射到 GUID 结构
        long lsb = UUID.randomUUID().getLeastSignificantBits();            
        
        return lsb;
    }

    public static long generateServerGuidHigh() {
        // 假设将 UUID 映射到 GUID 结构
        long msb = UUID.randomUUID().getMostSignificantBits();

        return msb;
    }

    // 0. 在类级别或方法开头定义随机数生成器 (建议使用 SecureRandom 更安全)
    private static final java.security.SecureRandom secureRandom = new java.security.SecureRandom();

    // 1. 生成指定长度的随机字节数组
    public static long  generateNewSessionId() {
        return secureRandom.nextLong() & 0x7FFFFFFFFFFFFFFFL; 
    }

    public static byte[] generateNtlmType2Message() {
        // 1. 准备基础数据
        byte[] serverChallenge = new byte[8];
        secureRandom.nextBytes(serverChallenge); // 生成 8 字节随机挑战码

        String targetName = "SMB-SERVER";
        byte[] targetNameBytes = targetName.getBytes(StandardCharsets.UTF_16LE);

        // 2. 构造 Target Info List (AV_PAIRs) —— Linux 挂载成功的关键
        Buffer avList = Buffer.buffer();
        
        // A. 添加服务器 NetBIOS 名 (ID: 0x0002)
        avList.appendShortLE((short) 0x0002); 
        avList.appendShortLE((short) targetNameBytes.length);
        avList.appendBytes(targetNameBytes);

        // B. 添加时间戳 (ID: 0x0007) —— 防止重放攻击，NTLMv2 必需
        avList.appendShortLE((short) 0x0007);
        avList.appendShortLE((short) 8);
        long fileTime = (System.currentTimeMillis() + 11644473600000L) * 10000L; // Unix 转 Windows FileTime
        avList.appendLongLE(fileTime);

        // C. 结束标志 (ID: 0x0000)
        avList.appendShortLE((short) 0x0000);
        avList.appendShortLE((short) 0);
        
        byte[] targetInfoBytes = avList.getBytes();

        // 3. 计算偏移量
        // NTLM 固定头部长度 = 48 字节 (Signature 8 + Type 4 + NamePtr 8 + Flags 4 + Challenge 8 + Reserved 8 + TargetInfoPtr 8)
        int headerSize = 48; 
        int targetNameOffset = headerSize;
        int targetInfoOffset = headerSize + targetNameBytes.length;

        // 4. 开始拼装最终的 NTLM Type 2 消息
        Buffer ntlmPacket = Buffer.buffer();

        // Signature: "NTLMSSP\0"
        ntlmPacket.appendBytes("NTLMSSP\0".getBytes(StandardCharsets.US_ASCII));

        // Message Type: 2 (Type 2 Challenge)
        ntlmPacket.appendIntLE(2);

        // Target Name Fields (Len, MaxLen, Offset)
        ntlmPacket.appendShortLE((short) targetNameBytes.length);
        ntlmPacket.appendShortLE((short) targetNameBytes.length);
        ntlmPacket.appendIntLE(targetNameOffset);

        // Negotiate Flags
        // 0x628a8215 是标准标志：支持 Unicode, NTLM2, TargetInfo, 128位加密等
        int flags = 0x628a8215; 
        ntlmPacket.appendIntLE(flags);

        // Server Challenge
        ntlmPacket.appendBytes(serverChallenge);

        // Reserved (8 bytes)
        ntlmPacket.appendLongLE(0L);

        // Target Info Fields (Len, MaxLen, Offset)
        ntlmPacket.appendShortLE((short) targetInfoBytes.length);
        ntlmPacket.appendShortLE((short) targetInfoBytes.length);
        ntlmPacket.appendIntLE(targetInfoOffset);

        // --- Payload 部分 ---
        // 写入实际的 Target Name
        ntlmPacket.appendBytes(targetNameBytes);
        // 写入实际的 Target Info List
        ntlmPacket.appendBytes(targetInfoBytes);

        return ntlmPacket.getBytes();
    }

    public static Buffer buildErrorResponse(Smb2Header requestHeader, long statusCode) {
        // 1. 构建 Header (64 bytes)
        Smb2Header errorHeader = new Smb2Header();
        errorHeader.setFlags(Smb2Constants.SMB2_FLAG_RESPONSE); // 必须标记为响应
        errorHeader.setCommand(requestHeader.getCommand());     // 命令字必须与请求一致
        errorHeader.setMessageId(requestHeader.getMessageId()); // 必须与请求 ID 一致
        errorHeader.setSessionId(requestHeader.getSessionId()); 
        errorHeader.setTreeId(requestHeader.getTreeId());
        errorHeader.setStatus(statusCode);                      // 设置错误状态码（如 0xC0000022）

        // 重要：必须分配 Credit（信用）。如果返回 0，客户端后续将无法发送请求，导致连接挂起。
        // 通常返回 1 或 8，取决于你的信用管理策略。
        errorHeader.setCreditRequestResponse((short) 1); // 分配 8 个信用

        Buffer headerBuffer = errorHeader.encode();

        // 2. 构建 SMB2 ERROR Response Body (固定 9 bytes)
        // 结构如下：
        // - StructureSize (2 bytes): 固定为 9 (0x0009)
        // - ErrorContextCount (1 byte): 通常为 0
        // - Reserved (1 byte): 固定 0
        // - ByteCount (4 bytes): 通常为 0
        // - ErrorData (1 byte): 填充位，通常为 0
        ByteBuffer errorBody = ByteBuffer.allocate(9).order(java.nio.ByteOrder.LITTLE_ENDIAN); 
        errorBody.putShort((short) 9);      // StructureSize: 0x0009
        errorBody.put((byte) 0);        // ErrorContextCount
        errorBody.put((byte) 0);        // Reserved
        errorBody.putInt(0);                // ByteCount
        errorBody.put((byte) 0);        // ErrorData / Padding

        // 3. 合并 Header 和 Body 并返回完整的响应

        // 4. 合并并添加 NBSS 长度头 (4字节，大端)
        Buffer fullPacket = Buffer.buffer();
        int totalBodySize = headerBuffer.length() + errorBody.array().length;

        // fullPacket.appendByte((byte) 0x00);      // Type: Session Message
        // fullPacket.appendMedium(totalBodySize);   // 24位长度 (大端)
        fullPacket.appendBuffer(headerBuffer);
        fullPacket.appendBytes(errorBody.array());

        return fullPacket;
    }

    public static long toFileTime(FileTime time) {
        if (time == null) return 0L;
        // 116444736000000000L 是 1601 到 1970 的纳秒偏移
        return (time.toMillis() * 10000L) + 116444736000000000L;
    }
}

