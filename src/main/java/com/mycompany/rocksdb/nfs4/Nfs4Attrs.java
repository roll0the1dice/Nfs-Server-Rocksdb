package com.mycompany.rocksdb.nfs4;

public class Nfs4Attrs {
    
    // --- Word 0: 基础与文件系统属性 ---
    public static final int FATTR4_SUPPORTED_ATTRS    = 1 << 0;  // 0:  查询支持的属性
    public static final int FATTR4_TYPE               = 1 << 1;  // 1:  文件类型 (Dir/File/Link...)
    public static final int FATTR4_FH_EXPIRE_TYPE     = 1 << 2;  // 2:  FH 过期类型
    public static final int FATTR4_CHANGE             = 1 << 3;  // 3:  Change ID (核心缓存校验)
    public static final int FATTR4_SIZE               = 1 << 4;  // 4:  文件大小
    public static final int FATTR4_LINK_SUPPORT       = 1 << 5;  // 5:  是否支持硬链接
    public static final int FATTR4_SYMLINK_SUPPORT    = 1 << 6;  // 6:  是否支持软链接
    public static final int FATTR4_NAMED_ATTR         = 1 << 7;  // 7:  是否有命名属性
    public static final int FATTR4_FSID               = 1 << 8;  // 8:  文件系统 ID
    public static final int FATTR4_UNIQUE_HANDLES     = 1 << 9;  // 9:  FH 是否唯一
    public static final int FATTR4_LEASE_TIME         = 1 << 10; // 10: 租约时间
    public static final int FATTR4_RDMA_ADDRS         = 1 << 11; // 11: RDMA 地址
    //public static final int FATTR4_FILEHANDLE         = 1 << 12; // 12: 文件句柄
    public static final int FATTR4_ACL                = 1 << 13; // 13: 访问控制列表
    public static final int FATTR4_ACLSUPPORT         = 1 << 14; // 14: ACL 支持类型
    public static final int FATTR4_ARCHIVE            = 1 << 15; // 15: 归档位
    public static final int FATTR4_CANSETTIME         = 1 << 16; // 16: 大小写不敏感
    public static final int FATTR4_CASE_INSENSITIVE   = 1 << 17; // 17: 大小写保留
    public static final int FATTR4_CASE_PRESERVING    = 1 << 18; // 18: Chown 受限
    public static final int FATTR4_FILEHANDLE         = 1 << 19; // 19: 获取当前 FH
    public static final int FATTR4_FILEID             = 1 << 20; // 20: File ID (Inode)
    public static final int FATTR4_FILES_AVAIL        = 1 << 21; // 21: 可用文件节点数
    public static final int FATTR4_FILES_FREE         = 1 << 22; // 22: 空闲文件节点数
    public static final int FATTR4_FILES_TOTAL        = 1 << 23; // 23: 总文件节点数
    public static final int FATTR4_FS_LOCATIONS       = 1 << 24; // 24: 文件系统位置 (迁移用)
    public static final int FATTR4_HIDDEN             = 1 << 25; // 25: 隐藏文件
    public static final int FATTR4_HOMOGENEOUS        = 1 << 26; // 26: 属性一致性
    public static final int FATTR4_MAXFILESIZE        = 1 << 27; // 27: 最大文件大小
    public static final int FATTR4_MAXLINK            = 1 << 28; // 28: 最大链接数
    public static final int FATTR4_MAXNAME            = 1 << 29; // 29: 最大文件名长度
    public static final int FATTR4_MAXREAD            = 1 << 30; // 30: 最大读取大小
    public static final int FATTR4_MAXWRITE           = 1 << 31; // 31: 最大写入大小

    // 
    public static final int FATTR4_MIMETYPE           = 1 << 0;  // 32: MIME 类型 (注意：这是符号位)
    // --- Word 1: UNIX 兼容、时间戳、空间 ---
    public static final int FATTR4_MODE               = 1 << 1;  // 33: 权限模式 (rwxrwxrwx)
    public static final int FATTR4_NO_TRUNC           = 1 << 2;  // 34: 文件名过长不截断
    public static final int FATTR4_NUMLINKS           = 1 << 3;  // 35: 硬链接数
    public static final int FATTR4_OWNER              = 1 << 4;  // 36: 所有者 (user@domain)
    public static final int FATTR4_OWNER_GROUP        = 1 << 5;  // 37: 所属组 (group@domain)
    public static final int FATTR4_QUOTA_AVAIL_HARD   = 1 << 6;  // 38: 硬配额剩余
    public static final int FATTR4_QUOTA_AVAIL_SOFT   = 1 << 7;  // 39: 软配额剩余
    public static final int FATTR4_QUOTA_USED         = 1 << 8;  // 40: 已用配额
    public static final int FATTR4_RAWDEV             = 1 << 9;  // 41: 原始设备号 (Major/Minor)
    public static final int FATTR4_SPACE_AVAIL        = 1 << 10;  // 42: 非特权用户可用空间
    public static final int FATTR4_SPACE_FREE         = 1 << 11; // 43: 总空闲空间
    public static final int FATTR4_SPACE_TOTAL        = 1 << 12; // 44: 总空间大小
    public static final int FATTR4_SPACE_USED         = 1 << 13; // 45: 文件占用空间 (blocks)
    public static final int FATTR4_SYSTEM             = 1 << 14; // 46: 系统文件
    public static final int FATTR4_TIME_ACCESS        = 1 << 15; // 47: 访问时间 (atime)
    public static final int FATTR4_TIME_ACCESS_SET    = 1 << 16; // 48: 设置 atime 方式
    public static final int FATTR4_TIME_BACKUP        = 1 << 17; // 49: 备份时间
    public static final int FATTR4_TIME_CREATE        = 1 << 18; // 50: 创建时间 (btime)
    public static final int FATTR4_TIME_DELTA         = 1 << 19; // 51: 时间精度
    public static final int FATTR4_TIME_METADATA      = 1 << 20; // 52: 元数据修改时间 (ctime)
    public static final int FATTR4_TIME_MODIFY        = 1 << 21; // 53: 内容修改时间 (mtime)
    public static final int FATTR4_TIME_MODIFY_SET    = 1 << 22; // 54: 设置 mtime 方式
    public static final int FATTR4_MOUNTED_ON_FILEID  = 1 << 23; // 55: 挂载点原始 FileID

    // --- NFSv4.1 新增 (Word 1) ---
    public static final int FATTR4_DIR_NOTIF_DELAY    = 1 << 24; // 56: 目录通知延迟
    public static final int FATTR4_DIRENT_NOTIF_DELAY = 1 << 25; // 57: 目录项通知延迟
    public static final int FATTR4_DACL               = 1 << 26; // 58: 自动继承 ACL (DACL)
    public static final int FATTR4_SACL               = 1 << 27; // 59: 系统审计 ACL (SACL)
    public static final int FATTR4_CHANGE_POLICY      = 1 << 28; // 60: 服务时间精度
    public static final int FATTR4_FS_STATUS          = 1 << 29; // 61: 服务端 access 时间
    public static final int FATTR4_FS_LAYOUT_TYPE     = 1 << 30; // 62: 服务端 modify 时间
    public static final int FATTR4_LAYOUT_HINT       = 1 << 31; // 63: AUTH_SYS 兼容 ACL

    public static final int FATTR4_LAYOUT_TYPE    = 1 << 0; // 64: 支持的布局类型 (pNFS)

    // --- NFSv4.1 扩展 (Word 2) ---
    public static final int FATTR4_LAYOUT_BLKSIZE        = 1 << 1;  // 65: 布局提示
    //public static final int FATTR4_LAYOUT_BLKSIZE        = 1 << 2;  // 66: 当前布局类型
    //public static final int FATTR4_LAYOUT_BLKSIZE     = 1 << 3;  // 67: 布局块大小
    public static final int FATTR4_LAYOUT_ALIGNMENT   = 1 << 4;  // 68: 布局对齐
    public static final int FATTR4_FS_LOCATIONS_INFO  = 1 << 5;  // 69: 扩展的文件系统位置信息
    public static final int FATTR4_MDSTHRESHOLD       = 1 << 6;  // 70: MDS I/O 阈值 (pNFS)
    public static final int FATTR4_RETENTION_GET      = 1 << 7;  // 71: 获取保留策略
    public static final int FATTR4_RETENTION_SET      = 1 << 8;  // 72: 设置保留策略
    public static final int FATTR4_RETENTEVT_GET      = 1 << 9;  // 73: 获取保留事件
    public static final int FATTR4_RETENTEVT_SET      = 1 << 10;  // 74: 设置保留事件
    public static final int FATTR4_SUPPATTR_EXCLCREAT     = 1 << 11; // 75: 排他创建支持的属性
    //public static final int FATTR4_SUPPATTR_EXCLCREAT    = 1 << 12; // 76: 掩码方式设置 Mode
    //public static final int FATTR4_SUPPATTR_EXCLCREAT = 1 << 13; // 77: 排他创建支持的属性
    public static final int FATTR4_FS_CHARSET_CAP     = 1 << 14; // 78: 文件名字符集能力

    // --- NFSv4.2 新增 (Word 2) ---
    public static final int FATTR4_CLONE_BLKSIZE      = 1 << 15; // 79: 克隆块大小 (Server-Side Copy)
    public static final int FATTR4_SPACE_FREED        = 1 << 16; // 80: 释放的空间 (用于打洞/Deallocate)
    public static final int FATTR4_CHANGE_ATTR_TYPE   = 1 << 17; // 81: Change 属性的类型 (单调/基于时间等)
    public static final int FATTR4_SEC_LABEL          = 1 << 18; // 82: 安全标签 (SELinux Label)
}
