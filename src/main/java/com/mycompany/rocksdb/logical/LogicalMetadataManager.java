package com.mycompany.rocksdb.logical;

import com.mycompany.rocksdb.POJO.ChunkFile;
import com.mycompany.rocksdb.POJO.Inode;
import com.mycompany.rocksdb.myrocksdb.MyRocksDB;
import com.mycompany.rocksdb.netserver.MountServer;
import com.mycompany.rocksdb.physical.PhysicalMetadataManager;
import com.mycompany.rocksdb.physical.PhysicalStorageEngine;
import com.mycompany.rocksdb.physical.PhysicalStorageEngine.PhysicalWriteResult;
import com.mycompany.rocksdb.utils.MetaKeyUtils;

import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogicalMetadataManager {
    private static final Logger log = LoggerFactory.getLogger(LogicalMetadataManager.class);

    /**
     * 极致优化写：支持物理数据写 与 逻辑空洞写（Punch Hole / Sparse Write）
     * 
     * @param offset 逻辑偏移
     * @param count  写入长度（如果是空洞，则代表空洞长度）
     * @param data   物理数据（如果是空洞写入，data 可为 null 或长度为 0）
     */
    public static void writeRangeOptimized(String bucket, String object, long offset, int count, byte[] data, Inode inode) {
        try {
            String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);
            String physicalFilename = null;

            // --- 阶段 1 & 2: 物理处理 (仅当有真实数据时执行) ---
            if (data != null && data.length > 0) {
                // 物理落盘
                log.info("log");
                PhysicalStorageEngine.PhysicalWriteResult writeResult = PhysicalStorageEngine.writeToPhysicalDevice(data);
                
                // 记录物理元数据
                physicalFilename = MetaKeyUtils.getObjFileName(bucket, object, MetaKeyUtils.getRequestId());
                String versionKey = MetaKeyUtils.getVersionMetaDataKey(targetVnodeId, bucket, object, inode.getVersionId());
                PhysicalMetadataManager.savePhysicalMetadata(physicalFilename, versionKey, writeResult, true);
            }

            // --- 阶段 3: 逻辑映射更新 (无论是数据还是空洞，逻辑映射都要更新) ---
            if (inode.getInodeData().isEmpty()) {
                handleNewFileInitialMapping(inode, physicalFilename, count, offset, targetVnodeId);
            } else {
                Inode.InodeData lastPtr = inode.getInodeData().get(inode.getInodeData().size() - 1);
                String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(bucket, lastPtr.fileName);
                ChunkFile chunkFile = MyRocksDB.getChunkFileMetaData(chunkKey)
                        .orElseThrow(() -> new RuntimeException("Chunk metadata lost: " + chunkKey));

                // 执行树扫描更新。注意：如果 physicalFilename 为 null，该方法内部会创建空洞段
                applyInplaceTreeScan(chunkFile.getChunkMap(), offset, count, physicalFilename);

                // 更新统计
                long newChunkSize = chunkFile.getActualSize();
                chunkFile.setSize(newChunkSize);
                lastPtr.setSize(newChunkSize);
                lastPtr.setChunkNum(chunkFile.getChunkMap().size());
                inode.setSize(Math.max(inode.getSize(), offset + count));

                // 持久化元数据
                MyRocksDB.saveChunkFileMetaData(chunkKey, chunkFile);
                MyRocksDB.saveINodeMetaData(targetVnodeId, inode);
            }
        } catch (Exception e) {
            throw new RuntimeException("Write range optimization failed", e);
        }
    }

    /**
     * TreeMap 原地修改算法
     * 复杂度: O(log N) 查找 + O(K) 局部清除 (K 为受影响的段数量)
     */
    private static void applyInplaceTreeScan(NavigableMap<Long, Inode.InodeData> map, long writeOffset, int writeLen, String physKey) {
        long writeEnd = writeOffset + writeLen;

        // 1. 补齐末尾空洞 (如果写入起点超过当前 EOF)
        long currentEOF = map.isEmpty() ? 0 : map.lastKey() + map.get(map.lastKey()).size;
        if (writeOffset > currentEOF) {
            map.put(currentEOF, Inode.InodeData.newHoleFile(writeOffset - currentEOF));
        }

        // 2. 定位受影响区间
        Map.Entry<Long, Inode.InodeData> firstOverlap = map.floorEntry(writeOffset);
        Map.Entry<Long, Inode.InodeData> lastOverlap = map.lowerEntry(writeEnd);

        // 3. 计算裁剪残余 (逻辑保持不变，支持数据或空洞的切分)
        Inode.InodeData leftRem = null;
        Long leftKey = null;
        if (firstOverlap != null) {
            long segStart = firstOverlap.getKey();
            long segEnd = segStart + firstOverlap.getValue().size;
            if (segEnd > writeOffset) {
                leftKey = segStart;
                leftRem = new Inode.InodeData(firstOverlap.getValue());
                leftRem.size = writeOffset - segStart;
            }
        }

        Inode.InodeData rightRem = null;
        if (lastOverlap != null) {
            long segStart = lastOverlap.getKey();
            long segEnd = segStart + lastOverlap.getValue().size;
            if (segEnd > writeEnd) {
                rightRem = new Inode.InodeData(lastOverlap.getValue());
                rightRem.offset += (writeEnd - segStart);
                rightRem.size = segEnd - writeEnd;
            }
        }

        // 4. 清理旧段 (O(log N))
        long deleteStart = (leftKey != null) ? leftKey : writeOffset;
        map.subMap(deleteStart, true, writeEnd, false).clear();

        // 5. 插入新段
        if (leftRem != null && leftRem.size > 0) map.put(leftKey, leftRem);
        
        // --- 核心改动：根据 physKey 决定插入类型 ---
        if (physKey != null) {
            // 插入真实数据段
            Inode.InodeData dataSeg = new Inode.InodeData();
            dataSeg.fileName = physKey;
            dataSeg.size = writeLen;
            dataSeg.offset = 0;
            map.put(writeOffset, dataSeg);
        } else {
            // 插入逻辑空洞段
            map.put(writeOffset, Inode.InodeData.newHoleFile(writeLen));
        }

        if (rightRem != null && rightRem.size > 0) map.put(writeEnd, rightRem);
    }
 
    /**
     * 处理全新文件（或首次逻辑映射）的初始化逻辑
     * 
     * @param inode           根 Inode 对象
     * @param physicalKey     物理数据段的标识 (由 PhysicalStorageEngine 生成)
     * @param len             数据物理长度
     * @param offset          逻辑起始偏移量 (如果 offset > 0，则自动产生起始空洞)
     * @param vnodeId         用于 RocksDB 存储的虚拟节点 ID
     */
    public static void handleNewFileInitialMapping(Inode inode, String physicalKey, long len, long offset, String vnodeId) {
        // 1. 生成新的 ChunkFile 唯一标识
        // 这里生成的 chunkFileName 会带有特定的前缀，以便 flatten 逻辑识别
        String chunkFileName = StringUtils.isBlank(physicalKey) ? MetaKeyUtils.getHoleFileName(inode.getBucket(), MetaKeyUtils.getRequestId()) : physicalKey;
        String chunkKey = ChunkFile.getChunkKey(chunkFileName);

        // 2. 初始化 ChunkFile 对象，直接使用 TreeMap
        ChunkFile chunkFile = ChunkFile.builder()
                .nodeId(inode.getNodeId())
                .bucket(inode.getBucket())
                .objName(inode.getObjName())
                .versionId(inode.getVersionId())
                .chunkMap(new TreeMap<>())
                .build();

        NavigableMap<Long, Inode.InodeData> map = chunkFile.getChunkMap();

        // 3. 处理起始空洞 (Leading Hole)
        // 如果 NFS 客户端直接在 offset 1024 写入，0-1024 必须被标记为 Hole
        if (offset > 0) {
            map.put(0L, Inode.InodeData.newHoleFile(offset));
        }

        // 4. 插入本次写入的物理片段或空洞
        Inode.InodeData newSeg = null;
        if (physicalKey != null) {
            newSeg = new Inode.InodeData();
            newSeg.fileName = physicalKey;
            newSeg.size = len;
            newSeg.offset = 0;
            map.put(offset, newSeg);
        } else {
            newSeg = Inode.InodeData.newHoleFile(len);
            map.put(offset, Inode.InodeData.newHoleFile(len));
        }

        assert newSeg != null;

        // 5. 更新 ChunkFile 统计信息
        long totalSize = offset + len;
        chunkFile.setSize(totalSize);

        // 6. 在 Inode 根列表中建立“指向该 ChunkFile”的逻辑指针
        // ChunkFile.newChunk 会封装 chunkFileName，使其在 flatten 时能被识别为 ChunkFile 引用
        Inode.InodeData chunkPointer = ChunkFile.newChunk(chunkFileName, newSeg, inode);
        chunkPointer.size = totalSize;
        chunkPointer.offset = 0; // 全局偏移从 0 开始
        chunkPointer.setChunkNum(map.size());

        // 7. 更新根 Inode 状态
        inode.getInodeData().add(chunkPointer);
        inode.setSize(totalSize);

        // 8. 原子/批量落盘元数据
        // 注意：顺序很重要，先存具体的分片容器，再存根索引
        MyRocksDB.saveChunkFileMetaData(chunkKey, chunkFile);
        MyRocksDB.saveINodeMetaData(vnodeId, inode);
    }

}
