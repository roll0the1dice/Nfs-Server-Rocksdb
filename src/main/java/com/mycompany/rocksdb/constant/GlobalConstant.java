package com.mycompany.rocksdb.constant;

public class GlobalConstant {
    public static final String ROCKS_FILE_SYSTEM_PREFIX = ".";
    // .1.
    public static final String ROCKS_FILE_SYSTEM_PREFIX_OFFSET = ROCKS_FILE_SYSTEM_PREFIX + "1" + ROCKS_FILE_SYSTEM_PREFIX;

    public static final String ROCKS_CHUNK_FILE_KEY = ")";

    public static final String ROCKS_INODE_PREFIX = "(";
    // 4KB
    public static final int BLOCK_SIZE = 4 * 1024 ;
    // 16 MB
    public static final int SPACE_SIZE = BLOCK_SIZE * BLOCK_SIZE;
    // 512
    public static final int SPACE_LEN = (int) SPACE_SIZE / 4096 / 8;

    public static final int VNODE_NUM = 65536;

    public static final int BLOCK_SIZE_4K = 4 * 1024;

    public static final String ROCKS_FILE_META_PREFIX = "#";
}
