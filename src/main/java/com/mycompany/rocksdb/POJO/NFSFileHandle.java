// src/main/java/com/macrosan/filesystem/nfs/types/NFSFileHandle.java
package com.mycompany.rocksdb.POJO;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * 一个全功能的NFS文件句柄类。
 * 采用统一的28字节格式，直接包含 Inode 和 FSID。
 * 格式: Header (12 bytes) + Inode (8 bytes) + FSID (8 bytes)
 */
public final class NFSFileHandle {

    // --- 常量 ---
    private static final int HANDLE_LENGTH_BYTES = 28;

    // --- 实例字段 ---
    private final long ino;
    private final long fsid;

    public NFSFileHandle(long ino, long fsid) {
        this.ino = ino;
        this.fsid = fsid;
    }

    // --- Getters ---
    public long getIno() { return ino; }
    public long getFsid() { return fsid; }

    // --- 编码方法 ---

    /**
     * 将当前对象编码为统一的28字节十六进制句柄字符串。
     * @return 56个字符的十六进制文件句柄。
     */
    public String toHexString() {
        ByteBuffer buffer = ByteBuffer.allocate(HANDLE_LENGTH_BYTES).order(ByteOrder.BIG_ENDIAN);

        // 写入12字节的固定头部
        buffer.putInt(0x01000701);
        buffer.putInt(0x02000002);
        buffer.putInt(0x00000000);

        // 写入8字节的 Inode Number
        buffer.putLong(this.ino);

        // 写入8字节的 Filesystem ID
        buffer.putLong(this.fsid);

        return bytesToHexString(buffer.array());
    }

    // --- 解码方法 ---

        /**
     * 【新方法】从28字节的二进制数组解码，创建NFSFileHandle实例。
     * @param handleBytes 包含文件句柄数据的28字节数组。
     * @return 一个新的 NFSFileHandle 实例。
     * @throws IllegalArgumentException 如果输入数组长度不正确。
     */
    public static NFSFileHandle fromHexArray(byte[] handleBytes) {
        if (handleBytes == null || handleBytes.length != HANDLE_LENGTH_BYTES) {
            throw new IllegalArgumentException("File handle byte array must be " + HANDLE_LENGTH_BYTES + " bytes long.");
        }

        ByteBuffer buffer = ByteBuffer.wrap(handleBytes).order(ByteOrder.BIG_ENDIAN);

        // 跳过12字节的头部
        buffer.position(12);

        // 读取8字节的 Inode
        long ino = buffer.getLong();

        // 读取8字节的 FSID
        long fsid = buffer.getLong();

        return new NFSFileHandle(ino, fsid);
    }

    /**
     * 从28字节的十六进制字符串解码，创建NFSFileHandle实例。
     * @param hexHandle 56个字符的十六进制文件句柄。
     * @return 一个新的 NFSFileHandle 实例。
     * @throws IllegalArgumentException 如果输入字符串格式不正确。
     */
    public static NFSFileHandle fromHexString(String hexHandle) {
        if (hexHandle == null || hexHandle.length() != HANDLE_LENGTH_BYTES * 2) {
            throw new IllegalArgumentException("File handle must be a " + (HANDLE_LENGTH_BYTES * 2) + "-character hex string.");
        }

        byte[] handleBytes = hexStringToByteArray(hexHandle);
        ByteBuffer buffer = ByteBuffer.wrap(handleBytes).order(ByteOrder.BIG_ENDIAN);

        // 跳过12字节的头部
        buffer.position(12);

        // 读取8字节的 Inode
        long ino = buffer.getLong();

        // 读取8字节的 FSID
        long fsid = buffer.getLong();

        return new NFSFileHandle(ino, fsid);
    }

    // --- 辅助方法 (未改变) ---
    private static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                                 + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    private static String bytesToHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
    
    // --- 标准 Object 方法 (未改变) ---
    @Override
    public String toString() {
        return "NFSFileHandle{" + "ino=" + ino + ", fsid=" + fsid + '}';
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NFSFileHandle that = (NFSFileHandle) o;
        return ino == that.ino && fsid == that.fsid;
    }
    @Override
    public int hashCode() {
        return Objects.hash(ino, fsid);
    }

    public static void main(String[] args) {
        System.out.println("--- Demonstrating Unified 28-byte File Handle (Header + Inode + FSID) ---");

        // 1. 定义一个文件/目录的属性
        long fileInode = 1152921504606846978L; // 一个大的示例inode: 0x1000000000000002
        long fileFsid = 2L;                   // 文件系统ID

        // 2. 创建代表该文件的NFSFileHandle对象
        NFSFileHandle originalHandle = new NFSFileHandle(fileInode, fileFsid);
        System.out.println("\nOriginal Object: " + originalHandle);

        // 3. 将对象编码为28字节的十六进制字符串
        String encodedHex = originalHandle.toHexString();
        
        // --- 手动构建预期的十六进制字符串用于验证 ---
        // Header: 010007010200000200000000
        // Inode (long): 1000000000000002
        // FSID  (long): 0000000000000002
        String expectedHex = "010007010200000200000000" + "1000000000000002" + "0000000000000002";
        
        System.out.println("\n--- ENCODING ---");
        System.out.println("Expected Hex: " + expectedHex);
        System.out.println("Encoded Hex:  " + encodedHex);
        assert expectedHex.equals(encodedHex);
        System.out.println("Encoding successful!");

        // 4. 从十六进制字符串解码回对象
        NFSFileHandle decodedHandle = NFSFileHandle.fromHexString(encodedHex);
        
        System.out.println("\n--- DECODING ---");
        System.out.println("Decoded Object: " + decodedHandle);

        // 5. 验证解码后的对象与原始对象完全相同
        assert originalHandle.equals(decodedHandle);
        System.out.println("Decoding successful! Decoded object matches the original.");
    }
}