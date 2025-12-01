package com.mycompany.rocksdb.nfs4.POJO;

import com.mycompany.rocksdb.POJO.Inode;
import com.mycompany.rocksdb.netserver.RpcConstants;
import com.mycompany.rocksdb.netserver.XdrUtils;
import com.mycompany.rocksdb.nfs4.Nfs4Attrs;
import com.mycompany.rocksdb.nfs4.NfsRequestContext;

import io.vertx.reactivex.core.buffer.Buffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class NfsFileAttributes {
    // 传入的数据
    // private final Inode inode;
    // private final int clientRequestMask0;
    // private final int clientRequestMask1;
    // private final int clientRequestMask2;

    // public NfsFileAttributes() {
    //     this.inode = inode;
    //     this.clientRequestMask0 = requestMask0;
    //     this.clientRequestMask1 = requestMask1;
    //     this.clientRequestMask2 = requestMask2;
    // }

    /**
     * 构建属性响应 Buffer
     *
     * @param inode              文件元数据对象
     * @param clientRequestMask0 客户端请求的第一组属性掩码 (Word 0)
     * @param clientRequestMask1 客户端请求的第二组属性掩码 (Word 1)
     * @return 编码好的 Buffer (不含 NFS4_OK 状态码，调用者需在前面先写入 0)
     */
    public static Buffer build(Inode inode, int[] clientRequestMask, NfsRequestContext nfsRequestContext) {
        // 1. 准备临时 Buffer 存放具体的属性值 (attr_vals)
        //    必须先写到这里，才能计算出总长度
        Buffer attrValsBuffer = Buffer.buffer();

        // 2. 初始化服务端实际返回的掩码 (Accumulators)
        //    一开始是 0，每成功写入一个属性，就对应位置 1

        int[] serverReturnMasks = new int[clientRequestMask.length];

        // =================================================================================
        // WORD 0 属性处理
        // 注意：必须严格按照 Bit 值从小到大顺序写入！
        // =================================================================================

        // Bit 0: FATTR4_SUPPORTED_ATTRS (查询支持的属性)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_SUPPORTED_ATTRS)) {
            // 返回服务器实际支持的属性掩码
            // Word 0 的支持属性
            int supportedMask0 = Nfs4Attrs.FATTR4_SUPPORTED_ATTRS
                                | Nfs4Attrs.FATTR4_TYPE
                                | Nfs4Attrs.FATTR4_CHANGE
                                | Nfs4Attrs.FATTR4_SIZE
                                | Nfs4Attrs.FATTR4_LINK_SUPPORT
                                | Nfs4Attrs.FATTR4_SYMLINK_SUPPORT
                                | Nfs4Attrs.FATTR4_NAMED_ATTR
                                | Nfs4Attrs.FATTR4_FSID
                                | Nfs4Attrs.FATTR4_UNIQUE_HANDLES
                                | Nfs4Attrs.FATTR4_LEASE_TIME
                                | Nfs4Attrs.FATTR4_RDMA_ADDRS
                                | Nfs4Attrs.FATTR4_FILEHANDLE
                                | Nfs4Attrs.FATTR4_ACL
                                | Nfs4Attrs.FATTR4_ACLSUPPORT
                                | Nfs4Attrs.FATTR4_ARCHIVE
                                | Nfs4Attrs.FATTR4_CANSETTIME
                                | Nfs4Attrs.FATTR4_CASE_INSENSITIVE
                                | Nfs4Attrs.FATTR4_CASE_PRESERVING
                                //| Nfs4Attrs.FATTR4_CHOWN_RESTRICTED
                                | Nfs4Attrs.FATTR4_FILEID
                                | Nfs4Attrs.FATTR4_FILES_AVAIL
                                | Nfs4Attrs.FATTR4_FILES_FREE
                                | Nfs4Attrs.FATTR4_FILES_TOTAL
                                | Nfs4Attrs.FATTR4_FS_LOCATIONS
                                | Nfs4Attrs.FATTR4_HIDDEN
                                | Nfs4Attrs.FATTR4_HOMOGENEOUS
                                | Nfs4Attrs.FATTR4_MAXFILESIZE
                                | Nfs4Attrs.FATTR4_MAXLINK
                                | Nfs4Attrs.FATTR4_MAXNAME
                                | Nfs4Attrs.FATTR4_MAXREAD
                                | Nfs4Attrs.FATTR4_MAXWRITE;

            // Word 1 的支持属性
            int supportedMask1 = Nfs4Attrs.FATTR4_MIMETYPE
                                | Nfs4Attrs.FATTR4_MODE
                                | Nfs4Attrs.FATTR4_NO_TRUNC
                                | Nfs4Attrs.FATTR4_NUMLINKS
                                | Nfs4Attrs.FATTR4_OWNER
                                | Nfs4Attrs.FATTR4_OWNER_GROUP
                                | Nfs4Attrs.FATTR4_QUOTA_AVAIL_HARD
                                | Nfs4Attrs.FATTR4_QUOTA_AVAIL_SOFT
                                | Nfs4Attrs.FATTR4_QUOTA_USED
                                | Nfs4Attrs.FATTR4_RAWDEV
                                | Nfs4Attrs.FATTR4_SPACE_AVAIL
                                | Nfs4Attrs.FATTR4_SPACE_FREE
                                | Nfs4Attrs.FATTR4_SPACE_TOTAL
                                | Nfs4Attrs.FATTR4_SPACE_USED
                                | Nfs4Attrs.FATTR4_SYSTEM
                                | Nfs4Attrs.FATTR4_TIME_ACCESS
                                | Nfs4Attrs.FATTR4_TIME_ACCESS_SET
                                | Nfs4Attrs.FATTR4_TIME_BACKUP
                                | Nfs4Attrs.FATTR4_TIME_CREATE
                                | Nfs4Attrs.FATTR4_TIME_DELTA
                                | Nfs4Attrs.FATTR4_TIME_METADATA
                                | Nfs4Attrs.FATTR4_TIME_MODIFY
                                | Nfs4Attrs.FATTR4_TIME_MODIFY_SET
                                | Nfs4Attrs.FATTR4_MOUNTED_ON_FILEID;

            // Word 2 及以后不支持，所以为 0
            int supportedMask2 = 0;

            // 写入 bitmap4 结构: count + [word0, word1, word2...]
            XdrUtils.writeInt(attrValsBuffer, 3); // count = 3 (Word 0, Word 1, Word 2)
            XdrUtils.writeInt(attrValsBuffer, supportedMask0);
            XdrUtils.writeInt(attrValsBuffer, supportedMask1); // 避免符号位问题
            XdrUtils.writeInt(attrValsBuffer, supportedMask2);

            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_SUPPORTED_ATTRS;
        }

        // Bit 1: NF4ATTR_TYPE (文件类型)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_TYPE)) {
            int type = inode.isDir() ? RpcConstants.NF4DIR : RpcConstants.NF4REG;
            XdrUtils.writeInt(attrValsBuffer, type);
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_TYPE;
        }

        // Bit 3: NF4ATTR_CHANGE (元数据变更标识)
        // 用于客户端缓存一致性检查，这里使用 mtime 作为简易实现
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_CHANGE)) {
            XdrUtils.writeLong(attrValsBuffer, inode.getMtime());
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_CHANGE;
        }

        // Bit 4: NF4ATTR_SIZE (文件大小)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_SIZE)) {
            XdrUtils.writeLong(attrValsBuffer, inode.getSize());
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_SIZE;
        }

        // Bit 5: NF4ATTR_LINK_SUPPORT (是否支持硬链接)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_LINK_SUPPORT)) {
            XdrUtils.writeBoolean(attrValsBuffer, true); // 假设支持硬链接
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_LINK_SUPPORT;
        }

        // Bit 6: NF4ATTR_SYMLINK_SUPPORT (是否支持软链接)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_SYMLINK_SUPPORT)) {
            XdrUtils.writeBoolean(attrValsBuffer, true); // 假设支持软链接
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_SYMLINK_SUPPORT;
        }

        // Bit 7: NF4ATTR_NAMED_ATTR (是否有命名属性)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_NAMED_ATTR)) {
            XdrUtils.writeBoolean(attrValsBuffer, false); // 假设不支持命名属性
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_NAMED_ATTR;
        }

        // Bit 8: NF4ATTR_FSID (文件系统 ID)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_FSID)) {
            // fsid4 结构包含两个 uint64 (major, minor)
            XdrUtils.writeLong(attrValsBuffer, 0L); // Major
            XdrUtils.writeLong(attrValsBuffer, 0L); // Minor
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_FSID;
        }

        // Bit 9: FATTR4_UNIQUE_HANDLES (FH 是否唯一)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_UNIQUE_HANDLES)) {
            XdrUtils.writeBoolean(attrValsBuffer, true);
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_UNIQUE_HANDLES;
        }

        // Bit 10: FATTR4_LEASE_TIME (租约时间)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_LEASE_TIME)) {
            XdrUtils.writeInt(attrValsBuffer, 90); // 暂定为0，表示无租约
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_LEASE_TIME;
        }

        // Bit 11: FATTR4_RDMA_ADDRS (RDMA 地址)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_RDMA_ADDRS)) {
            // rdma_addrs4 结构，这里返回空列表
            XdrUtils.writeInt(attrValsBuffer, 0); // count = 0
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_RDMA_ADDRS;
        }

        // Bit 12: FATTR4_FILEHANDLE (文件句柄)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_FILEHANDLE)) {
            // 这里返回空的filehandle，实际应该返回对应文件的filehandle
            long inodeId = nfsRequestContext.getState().getCurrentInodeId();
            byte[] bytesBig = ByteBuffer.allocate(8).putLong(inodeId).array();
            XdrUtils.writeOpaque(attrValsBuffer, bytesBig); // opaque bytes count
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_FILEHANDLE;
        }

        // Bit 13: FATTR4_ACL (访问控制列表)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_ACL)) {
            XdrUtils.writeInt(attrValsBuffer, 0); // acl count = 0
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_ACL;
        }

        // Bit 14: FATTR4_ACLSUPPORT (ACL 支持类型)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_ACLSUPPORT)) {
            XdrUtils.writeInt(attrValsBuffer, 0); // No ACL support
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_ACLSUPPORT;
        }

        // Bit 15: FATTR4_ARCHIVE (归档位)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_ARCHIVE)) {
            XdrUtils.writeBoolean(attrValsBuffer, false);
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_ARCHIVE;
        }

        // Bit 16: FATTR4_CANSETTIME (是否可以设置时间)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_CANSETTIME)) {
            XdrUtils.writeBoolean(attrValsBuffer, true);
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_CANSETTIME;
        }

        // Bit 17: FATTR4_CASE_INSENSITIVE (大小写不敏感)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_CASE_INSENSITIVE)) {
            XdrUtils.writeBoolean(attrValsBuffer, false);
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_CASE_INSENSITIVE;
        }

        // Bit 18: FATTR4_CASE_PRESERVING (大小写保留)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_CASE_PRESERVING)) {
            XdrUtils.writeBoolean(attrValsBuffer, true);
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_CASE_PRESERVING;
        }

        // Bit 19: FATTR4_CHOWN_RESTRICTED (Chown 受限)
        // if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_CHOWN_RESTRICTED)) {
        //     XdrUtils.writeBoolean(attrValsBuffer, true);
        //     serverReturnMasks[0] |= Nfs4Attrs.FATTR4_CHOWN_RESTRICTED;
        // }

        // Bit 20: NF4ATTR_FILEID (Inode ID)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_FILEID)) {
            XdrUtils.writeLong(attrValsBuffer, inode.getInodeId());
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_FILEID;
        }

        // Bit 21: FATTR4_FILES_AVAIL (可用文件节点数)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_FILES_AVAIL)) {
            XdrUtils.writeLong(attrValsBuffer, 0L); // 假设无限
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_FILES_AVAIL;
        }

        // Bit 22: FATTR4_FILES_FREE (空闲文件节点数)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_FILES_FREE)) {
            XdrUtils.writeLong(attrValsBuffer, 0L); // 假设无限
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_FILES_FREE;
        }

        // Bit 23: FATTR4_FILES_TOTAL (总文件节点数)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_FILES_TOTAL)) {
            XdrUtils.writeLong(attrValsBuffer, 0L); // 假设无限
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_FILES_TOTAL;
        }

        // Bit 24: FATTR4_FS_LOCATIONS (文件系统位置 (迁移用))
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_FS_LOCATIONS)) {
            XdrUtils.writeInt(attrValsBuffer, 0); // count = 0
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_FS_LOCATIONS;
        }

        // Bit 25: FATTR4_HIDDEN (隐藏文件)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_HIDDEN)) {
            XdrUtils.writeBoolean(attrValsBuffer, false);
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_HIDDEN;
        }

        // Bit 26: FATTR4_HOMOGENEOUS (属性一致性)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_HOMOGENEOUS)) {
            XdrUtils.writeBoolean(attrValsBuffer, true);
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_HOMOGENEOUS;
        }

        // Bit 27: FATTR4_MAXFILESIZE (最大文件大小)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_MAXFILESIZE)) {
            XdrUtils.writeLong(attrValsBuffer, Long.MAX_VALUE);
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_MAXFILESIZE;
        }

        // Bit 28: FATTR4_MAXLINK (最大链接数)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_MAXLINK)) {
            XdrUtils.writeInt(attrValsBuffer, 255);
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_MAXLINK;
        }

        // Bit 29: FATTR4_MAXNAME (最大文件名长度)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_MAXNAME)) {
            XdrUtils.writeInt(attrValsBuffer, 255);
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_MAXNAME;
        }

        // Bit 30: FATTR4_MAXREAD (最大读取大小)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_MAXREAD)) {
            XdrUtils.writeLong(attrValsBuffer, 1024 * 1024 * 4L); // 4MB
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_MAXREAD;
        }

        // Bit 31: FATTR4_MAXWRITE (最大写入大小)
        if (isRequested(clientRequestMask, 0, Nfs4Attrs.FATTR4_MAXWRITE)) {
            XdrUtils.writeLong(attrValsBuffer, 1024 * 1024 * 4L); // 4MB
            serverReturnMasks[0] |= Nfs4Attrs.FATTR4_MAXWRITE;
        }

        // =================================================================================
        // WORD 1 属性处理
        // 这里的 Bit 值实际上是 (1 << (N - 32))
        // =================================================================================

        // Bit 32 (Word 1, Bit 0): FATTR4_MIMETYPE (MIME 类型)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_MIMETYPE)) {
            writeXdrString(attrValsBuffer, "application/octet-stream"); // 默认二进制流
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_MIMETYPE;
        }

        // Bit 33 (Word 1, Bit 1): NF4ATTR_MODE (权限模式)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_MODE)) {
            XdrUtils.writeInt(attrValsBuffer, inode.getMode()); // 仅低12位有效
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_MODE;
        }

        // Bit 34 (Word 1, Bit 2): FATTR4_NO_TRUNC (文件名过长不截断)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_NO_TRUNC)) {
            XdrUtils.writeBoolean(attrValsBuffer, true);
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_NO_TRUNC;
        }

        // Bit 35 (Word 1, Bit 3): NF4ATTR_NUMLINKS (硬链接数)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_NUMLINKS)) {
            // 目录至少有2个链接 (. 和 ..)，普通文件通常是1
            XdrUtils.writeInt(attrValsBuffer, inode.getLinkN());
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_NUMLINKS;
        }

        // Bit 36 (Word 1, Bit 4): NF4ATTR_OWNER (所有者用户名)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_OWNER)) {
            // NFSv4 传输的是字符串形式的用户名，如 "root" 或 "user@domain"
            // 这里暂时硬编码为 root，实际应查用户表
            writeXdrString(attrValsBuffer, "65534");
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_OWNER;
        }

        // Bit 37 (Word 1, Bit 5): NF4ATTR_OWNER_GROUP (所属组名)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_OWNER_GROUP)) {
            writeXdrString(attrValsBuffer, "65534");
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_OWNER_GROUP;
        }

        // Bit 38 (Word 1, Bit 6): FATTR4_QUOTA_AVAIL_HARD (硬配额剩余)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_QUOTA_AVAIL_HARD)) {
            XdrUtils.writeLong(attrValsBuffer, 0L);
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_QUOTA_AVAIL_HARD;
        }

        // Bit 39 (Word 1, Bit 7): FATTR4_QUOTA_AVAIL_SOFT (软配额剩余)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_QUOTA_AVAIL_SOFT)) {
            XdrUtils.writeLong(attrValsBuffer, 0L);
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_QUOTA_AVAIL_SOFT;
        }

        // Bit 40 (Word 1, Bit 8): FATTR4_QUOTA_USED (已用配额)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_QUOTA_USED)) {
            XdrUtils.writeLong(attrValsBuffer, 0L);
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_QUOTA_USED;
        }

        // Bit 41 (Word 1, Bit 9): NF4ATTR_RAWDEV (设备号)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_RAWDEV)) {
            // specdata4 (uint32 specdata1, uint32 specdata2)
            XdrUtils.writeInt(attrValsBuffer, 0);
            XdrUtils.writeInt(attrValsBuffer, 0);
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_RAWDEV;
        }

        // Bit 42 (Word 1, Bit 10): FATTR4_SPACE_AVAIL (非特权用户可用空间)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_SPACE_AVAIL)) {
            XdrUtils.writeLong(attrValsBuffer, 0L);
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_SPACE_AVAIL;
        }

        // Bit 43 (Word 1, Bit 11): FATTR4_SPACE_FREE (总空闲空间)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_SPACE_FREE)) {
            XdrUtils.writeLong(attrValsBuffer, 0L);
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_SPACE_FREE;
        }

        // Bit 44 (Word 1, Bit 12): FATTR4_SPACE_TOTAL (总空间大小)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_SPACE_TOTAL)) {
            XdrUtils.writeLong(attrValsBuffer, 0L);
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_SPACE_TOTAL;
        }

        // Bit 45 (Word 1, Bit 13): NF4ATTR_SPACE_USED (文件占用空间)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_SPACE_USED)) {
            // 简单实现：等于文件大小。精确实现应为 block_size * blocks
            XdrUtils.writeLong(attrValsBuffer, inode.getSize());
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_SPACE_USED;
        }

        // Bit 46 (Word 1, Bit 14): FATTR4_SYSTEM (系统文件)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_SYSTEM)) {
            XdrUtils.writeBoolean(attrValsBuffer, false);
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_SYSTEM;
        }

        // Bit 47 (Word 1, Bit 15): NF4ATTR_TIME_ACCESS (访问时间)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_TIME_ACCESS)) {
            XdrUtils.writeLong(attrValsBuffer, inode.getAtime());
            XdrUtils.writeInt(attrValsBuffer, inode.getAtimensec());
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_TIME_ACCESS;
        }

        // Bit 48 (Word 1, Bit 16): FATTR4_TIME_ACCESS_SET (设置 atime 方式)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_TIME_ACCESS_SET)) {
            XdrUtils.writeInt(attrValsBuffer, 0); // SET_TO_SERVER_TIME
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_TIME_ACCESS_SET;
        }

        // Bit 49 (Word 1, Bit 17): FATTR4_TIME_BACKUP (备份时间)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_TIME_BACKUP)) {
            XdrUtils.writeLong(attrValsBuffer, 0L);
            XdrUtils.writeInt(attrValsBuffer, 0);
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_TIME_BACKUP;
        }

        // Bit 50 (Word 1, Bit 18): FATTR4_TIME_CREATE (创建时间 - btime)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_TIME_CREATE)) {
            XdrUtils.writeLong(attrValsBuffer, inode.getCtime());
            XdrUtils.writeInt(attrValsBuffer, inode.getCtimensec());
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_TIME_CREATE;
        }

        // Bit 51 (Word 1, Bit 19): FATTR4_TIME_DELTA (时间精度)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_TIME_DELTA)) {
            XdrUtils.writeLong(attrValsBuffer, 1L); // 1秒精度
            XdrUtils.writeInt(attrValsBuffer, 0);
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_TIME_DELTA;
        }

        // Bit 52 (Word 1, Bit 20): NF4ATTR_TIME_METADATA (元数据修改时间 - ctime)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_TIME_METADATA)) {
            // 假设 Inode 中只存储了毫秒级时间，纳秒部分传 0
            XdrUtils.writeLong(attrValsBuffer, inode.getCtime());
            XdrUtils.writeInt(attrValsBuffer, inode.getCtimensec());
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_TIME_METADATA;
        }

        // Bit 53 (Word 1, Bit 21): NF4ATTR_TIME_MODIFY (内容修改时间 - mtime)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_TIME_MODIFY)) {
            XdrUtils.writeLong(attrValsBuffer, inode.getMtime());
            XdrUtils.writeInt(attrValsBuffer, inode.getMtimensec());
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_TIME_MODIFY;
        }

        // Bit 54 (Word 1, Bit 22): FATTR4_TIME_MODIFY_SET (设置 mtime 方式)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_TIME_MODIFY_SET)) {
            XdrUtils.writeInt(attrValsBuffer, 0); // SET_TO_SERVER_TIME
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_TIME_MODIFY_SET;
        }

        // Bit 55 (Word 1, Bit 23): FATTR4_MOUNTED_ON_FILEID (挂载点原始 FileID)
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_MOUNTED_ON_FILEID)) {
            XdrUtils.writeLong(attrValsBuffer,  inode.getInodeId());
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_MOUNTED_ON_FILEID;
        }

        // --- NFSv4.1 新增 (Word 1) ---
        // Bit 56 (Word 1, Bit 24): FATTR4_DIR_NOTIF_DELAY (目录通知延迟)
        // if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_DIR_NOTIF_DELAY)) {
        //     XdrUtils.writeLong(attrValsBuffer, 0L);
        //     XdrUtils.writeInt(attrValsBuffer, 0);
        //     serverReturnMasks[1] |= Nfs4Attrs.FATTR4_DIR_NOTIF_DELAY;
        // }

        // Bit 57 (Word 1, Bit 25): FATTR4_DIRENT_NOTIF_DELAY (目录项通知延迟)
        // if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_DIRENT_NOTIF_DELAY)) {
        //     XdrUtils.writeLong(attrValsBuffer, 0L);
        //     XdrUtils.writeInt(attrValsBuffer, 0);
        //     serverReturnMasks[1] |= Nfs4Attrs.FATTR4_DIRENT_NOTIF_DELAY;
        // }

        // Bit 58 (Word 1, Bit 26): FATTR4_DACL (自动继承 ACL (DACL))
        // if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_DACL)) {
        //     XdrUtils.writeInt(attrValsBuffer, 0); // acl count = 0
        //     serverReturnMasks[1] |= Nfs4Attrs.FATTR4_DACL;
        // }

        // // Bit 59 (Word 1, Bit 27): FATTR4_SACL (系统审计 ACL (SACL))
        // if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_SACL)) {
        //     XdrUtils.writeInt(attrValsBuffer, 0); // acl count = 0
        //     serverReturnMasks[1] |= Nfs4Attrs.FATTR4_SACL;
        // }

        // // Bit 60 (Word 1, Bit 28): FATTR4_SERVICE_TIME_DELTA (服务时间精度)
        // if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_SERVICE_TIME_DELTA)) {
        //     XdrUtils.writeLong(attrValsBuffer, 1L); // 1秒精度
        //     XdrUtils.writeInt(attrValsBuffer, 0);
        //     serverReturnMasks[1] |= Nfs4Attrs.FATTR4_SERVICE_TIME_DELTA;
        // }

        // // Bit 61 (Word 1, Bit 29): FATTR4_SERVICE_TIME_ACCESS (服务端 access 时间)
        // if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_SERVICE_TIME_ACCESS)) {
        //     XdrUtils.writeLong(attrValsBuffer, 0L);
        //     XdrUtils.writeInt(attrValsBuffer, 0);
        //     serverReturnMasks[1] |= Nfs4Attrs.FATTR4_SERVICE_TIME_ACCESS;
        // }

        // Bit 62 (Word 1, Bit 30): FATTR4_SERVICE_TIME_MODIFY (服务端 modify 时间)
        //int xx = clientRequestMask[1];
        //if (xx != 0) {}
        if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_FS_LAYOUT_TYPE)) {
            XdrUtils.writeInt(attrValsBuffer, 0);
            serverReturnMasks[1] |= Nfs4Attrs.FATTR4_FS_LAYOUT_TYPE;
        }

        // // Bit 63 (Word 1, Bit 31): FATTR4_AUTH_SYS_ACL (AUTH_SYS 兼容 ACL)
        // if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_AUTH_SYS_ACL)) {
        //     XdrUtils.writeInt(attrValsBuffer, 0); // No ACL support
        //     serverReturnMasks[1] |= Nfs4Attrs.FATTR4_AUTH_SYS_ACL;
        // }

        // // Bit 64 (Word 1, Bit 32): FATTR4_FS_LAYOUT_TYPES (支持的布局类型 (pNFS))
        // if (isRequested(clientRequestMask, 1, Nfs4Attrs.FATTR4_FS_LAYOUT_TYPES)) {
        //     XdrUtils.writeInt(attrValsBuffer, 1); // Array count
        //     XdrUtils.writeInt(attrValsBuffer, 1); // LAYOUT4_NFSV4_1_FILES
        //     serverReturnMasks[1] |= Nfs4Attrs.FATTR4_FS_LAYOUT_TYPES;
        // }


        // // =====================================================================
        // // Word 2 处理 (pNFS & Advanced Features)
        // // =====================================================================

        // // Bit 64 (Word 2, Bit 0): FATTR4_LAYOUT_HINT (布局提示)
        // if (isRequested(clientRequestMask, 2, Nfs4Attrs.FATTR4_LAYOUT_HINT)) {
        //     XdrUtils.writeInt(attrValsBuffer, 0); // layout_hint4结构，简单返回0
        //     serverReturnMasks[2] |= Nfs4Attrs.FATTR4_LAYOUT_HINT;
        // }

        // // Bit 65 (Word 2, Bit 1): FATTR4_LAYOUT_TYPE (当前布局类型)
        // if (isRequested(clientRequestMask, 2, Nfs4Attrs.FATTR4_LAYOUT_TYPE)) {
        //     XdrUtils.writeInt(attrValsBuffer, 0); // layouttype4，简单返回0
        //     serverReturnMasks[2] |= Nfs4Attrs.FATTR4_LAYOUT_TYPE;
        // }

        // // Bit 66 (Word 2, Bit 2): FATTR4_LAYOUT_BLKSIZE (布局块大小)
        // if (isRequested(clientRequestMask, 2, Nfs4Attrs.FATTR4_LAYOUT_BLKSIZE)) {
        //     XdrUtils.writeInt(attrValsBuffer, 4096);
        //     serverReturnMasks[2] |= Nfs4Attrs.FATTR4_LAYOUT_BLKSIZE;
        // }

        // // Bit 67 (Word 2, Bit 3): FATTR4_LAYOUT_ALIGNMENT (布局对齐)
        // if (isRequested(clientRequestMask, 2, Nfs4Attrs.FATTR4_LAYOUT_ALIGNMENT)) {
        //     XdrUtils.writeInt(attrValsBuffer, 4096);
        //     serverReturnMasks[2] |= Nfs4Attrs.FATTR4_LAYOUT_ALIGNMENT;
        // }

        // // Bit 68 (Word 2, Bit 4): FATTR4_FS_LOCATIONS_INFO (扩展的文件系统位置信息)
        // if (isRequested(clientRequestMask, 2, Nfs4Attrs.FATTR4_FS_LOCATIONS_INFO)) {
        //     XdrUtils.writeInt(attrValsBuffer, 0); // fsl_locations_info4结构，简单返回0
        //     serverReturnMasks[2] |= Nfs4Attrs.FATTR4_FS_LOCATIONS_INFO;
        // }

        // // Bit 69 (Word 2, Bit 5): FATTR4_MDSTHRESHOLD (MDS I/O 阈值 (pNFS))
        // if (isRequested(clientRequestMask, 2, Nfs4Attrs.FATTR4_MDSTHRESHOLD)) {
        //     XdrUtils.writeInt(attrValsBuffer, 0); // mdsthreshold4结构，简单返回0
        //     serverReturnMasks[2] |= Nfs4Attrs.FATTR4_MDSTHRESHOLD;
        // }

        // // Bit 70 (Word 2, Bit 6): FATTR4_RETENTION_GET (获取保留策略)
        // if (isRequested(clientRequestMask, 2, Nfs4Attrs.FATTR4_RETENTION_GET)) {
        //     XdrUtils.writeInt(attrValsBuffer, 0); // retention_get4结构，简单返回0
        //     serverReturnMasks[2] |= Nfs4Attrs.FATTR4_RETENTION_GET;
        // }

        // // Bit 71 (Word 2, Bit 7): FATTR4_RETENTION_SET (设置保留策略)
        // if (isRequested(clientRequestMask, 2, Nfs4Attrs.FATTR4_RETENTION_SET)) {
        //     XdrUtils.writeInt(attrValsBuffer, 0); // retention_set4结构，简单返回0
        //     serverReturnMasks[2] |= Nfs4Attrs.FATTR4_RETENTION_SET;
        // }

        // // Bit 72 (Word 2, Bit 8): FATTR4_RETENTEVT_GET (获取保留事件)
        // if (isRequested(clientRequestMask, 2, Nfs4Attrs.FATTR4_RETENTEVT_GET)) {
        //     XdrUtils.writeInt(attrValsBuffer, 0); // retentevt_get4结构，简单返回0
        //     serverReturnMasks[2] |= Nfs4Attrs.FATTR4_RETENTEVT_GET;
        // }

        // // Bit 73 (Word 2, Bit 9): FATTR4_RETENTEVT_SET (设置保留事件)
        // if (isRequested(clientRequestMask, 2, Nfs4Attrs.FATTR4_RETENTEVT_SET)) {
        //     XdrUtils.writeInt(attrValsBuffer, 0); // retentevt_set4结构，简单返回0
        //     serverReturnMasks[2] |= Nfs4Attrs.FATTR4_RETENTEVT_SET;
        // }

        // // Bit 74 (Word 2, Bit 10): FATTR4_RETENTION_HOLD (保留持有状态)
        // if (isRequested(clientRequestMask, 2, Nfs4Attrs.FATTR4_RETENTION_HOLD)) {
        //     XdrUtils.writeBoolean(attrValsBuffer, false);
        //     serverReturnMasks[2] |= Nfs4Attrs.FATTR4_RETENTION_HOLD;
        // }

        // // Bit 75 (Word 2, Bit 11): FATTR4_MODE_SET_MASKED (掩码方式设置 Mode)
        // if (isRequested(clientRequestMask, 2, Nfs4Attrs.FATTR4_MODE_SET_MASKED)) {
        //     XdrUtils.writeInt(attrValsBuffer, 0); // mode_set_masked4结构，简单返回0
        //     serverReturnMasks[2] |= Nfs4Attrs.FATTR4_MODE_SET_MASKED;
        // }

        // // Bit 76 (Word 2, Bit 12): FATTR4_SUPPATTR_EXCLCREAT (排他创建支持的属性)
        // if (isRequested(clientRequestMask, 2, Nfs4Attrs.FATTR4_SUPPATTR_EXCLCREAT)) {
        //     XdrUtils.writeInt(attrValsBuffer, 0); // bitmap4结构，简单返回0
        //     serverReturnMasks[2] |= Nfs4Attrs.FATTR4_SUPPATTR_EXCLCREAT;
        // }

        // // Bit 77 (Word 2, Bit 13): FATTR4_FS_CHARSET_CAP (文件名字符集能力)
        // if (isRequested(clientRequestMask, 2, Nfs4Attrs.FATTR4_FS_CHARSET_CAP)) {
        //     XdrUtils.writeInt(attrValsBuffer, 0); // fs_charset_cap4结构，简单返回0
        //     serverReturnMasks[2] |= Nfs4Attrs.FATTR4_FS_CHARSET_CAP;
        // }

        // // --- NFSv4.2 新增 (Word 2) ---
        // // Bit 78 (Word 2, Bit 14): FATTR4_CLONE_BLKSIZE (克隆块大小 (Server-Side Copy))
        // if (isRequested(clientRequestMask, 2, Nfs4Attrs.FATTR4_CLONE_BLKSIZE)) {
        //     XdrUtils.writeLong(attrValsBuffer, 0L);
        //     serverReturnMasks[2] |= Nfs4Attrs.FATTR4_CLONE_BLKSIZE;
        // }

        // // Bit 79 (Word 2, Bit 15): FATTR4_SPACE_FREED (释放的空间 (用于打洞/Deallocate))
        // if (isRequested(clientRequestMask, 2, Nfs4Attrs.FATTR4_SPACE_FREED)) {
        //     XdrUtils.writeLong(attrValsBuffer, 0L);
        //     serverReturnMasks[2] |= Nfs4Attrs.FATTR4_SPACE_FREED;
        // }

        // // Bit 80 (Word 2, Bit 16): FATTR4_CHANGE_ATTR_TYPE (Change 属性的类型 (单调/基于时间等))
        // if (isRequested(clientRequestMask, 2, Nfs4Attrs.FATTR4_CHANGE_ATTR_TYPE)) {
        //     XdrUtils.writeInt(attrValsBuffer, 0); // change_attr_type4结构，简单返回0
        //     serverReturnMasks[2] |= Nfs4Attrs.FATTR4_CHANGE_ATTR_TYPE;
        // }

        // // Bit 81 (Word 2, Bit 17): FATTR4_SEC_LABEL (安全标签 (SELinux Label))
        // if (isRequested(clientRequestMask, 2, Nfs4Attrs.FATTR4_SEC_LABEL)) {
        //     XdrUtils.writeInt(attrValsBuffer, 0); // sec_label4结构，简单返回0
        //     serverReturnMasks[2] |= Nfs4Attrs.FATTR4_SEC_LABEL;
        // }

        // =================================================================================
        // 最终报文组装
        // =================================================================================
        Buffer finalResponseBuffer = Buffer.buffer();

        // 1. 写入 Bitmap 数组长度 (bitmap4 count)
        // 即使某个 Word 是 0，也要写出来占位，通常 NFSv4 都是返回 2 个 Word
        XdrUtils.writeInt(finalResponseBuffer, clientRequestMask.length);

        // 2. 写入实际计算出的返回掩码 (bitmap4 value)
        // 告诉客户端：刚才你求的那些，我实际上给了这些
        for (int i = 0; i < clientRequestMask.length; i++) {
            // 防止数组越界：如果算出来的 length 比实际数组大，补 0
            XdrUtils.writeInt(finalResponseBuffer, serverReturnMasks[i]);
        }

        // 3. 写入属性列表总长度 (attrlist4 length)
        // 这是后续数据流的字节数
        XdrUtils.writeInt(finalResponseBuffer, attrValsBuffer.length());

        // 4. 写入属性数据 (attrlist4 data)
        finalResponseBuffer.appendBuffer(attrValsBuffer);

        return finalResponseBuffer;
    }

    private static void writeXdrString(Buffer buffer, String val) {
        if (val == null) val = "";
        byte[] bytes = val.getBytes(StandardCharsets.UTF_8);

        // 1. 必须先写 4字节的长度！
        XdrUtils.writeInt(buffer, bytes.length);

        // 2. 写入字符串内容
        buffer.appendBytes(bytes);

        // 3. 必须计算 Padding (对齐到4字节)
        // 比如 "root" 长度4，padding=0
        // 比如 "nfs"  长度3，padding=1
        int padding = (4 - (bytes.length % 4)) % 4;
        for (int i = 0; i < padding; i++) {
            buffer.appendByte((byte) 0);
        }
    }

    /**
     * 判断客户端是否请求了特定的属性
     * @param reqBitmaps 客户端传来的 bitmap 数组 (变长)
     * @param wordIdx    要检查的 Word 索引 (0, 1, 2)
     * @param bitMask    要检查的位掩码 (例如 F4A_SIZE)
     */
    private static boolean isRequested(int[] reqBitmaps, int wordIdx, int bitMask) {
        // 1. 如果代码想查 Word 2，但客户端只传了 [Word0, Word1]，直接返回 false
        if (reqBitmaps == null || wordIdx >= reqBitmaps.length) {
            return false;
        }
        // 2. 否则进行正常的位与运算
        return (reqBitmaps[wordIdx] & bitMask) != 0;
    }

    // --- 辅助方法，让主逻辑更干净 ---

    // private boolean hasRequest(int wordIndex, int mask) {
    //     int req = (wordIndex == 0) ? requestMask0 : requestMask1;
    //     return (req & mask) != 0;
    // }

    // private void markResult(int wordIndex, int mask) {
    //     if (wordIndex == 0) resultMask0 |= mask;
    //     else resultMask1 |= mask;
    // }

    // private void writeString(Buffer buf, String s) {
    //     byte[] b = s.getBytes(StandardCharsets.UTF_8);
    //     XdrUtils.writeOpaque(buf, b);
    // }

    // private void writeTime(Buffer buf, long millis, long nanos) {
    //     XdrUtils.writeLong(buf, millis / 1000); // seconds
    //     XdrUtils.writeInt(buf, (int) (nanos % 1_000_000_000)); // nanoseconds
    // }
}