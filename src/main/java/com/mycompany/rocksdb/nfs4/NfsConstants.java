package com.mycompany.rocksdb.nfs4;

public class NfsConstants {
    public static final String hexString = "0100010000000000";
    // 根节点的固定 ID
    public static final long ROOT_INODE_ID = Long.parseUnsignedLong(hexString, 16);
    
    // 根节点的 FileHandle (字节数组)，可以简单地就是 ID 的序列化
    // 假设用 8 字节表示
    public static final byte[] ROOT_FILE_HANDLE = new byte[]{0, 0, 0, 0, 0, 0, 0, 1}; 
}