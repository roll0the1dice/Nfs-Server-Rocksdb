package com.mycompany.rocksdb.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.mycompany.rocksdb.POJO.ChunkFile;
import com.mycompany.rocksdb.POJO.FileMetadata;
import com.mycompany.rocksdb.POJO.Inode;
import com.mycompany.rocksdb.logical.LogicalMetadataManager;
import com.mycompany.rocksdb.myrocksdb.MyRocksDB;
import com.mycompany.rocksdb.physical.PhysicalStorageEngine;

import static com.mycompany.rocksdb.constant.GlobalConstant.ROCKS_CHUNK_FILE_KEY;

public class FSUtils2 {
    /**
     * 基于 TreeMap 的递归展开与过滤
     * 
     * @param bucket       存储桶
     * @param currentMap   当前层级的逻辑段 Map (Key: 当前层级的相对偏移)
     * @param readOffset   请求在该层级的起始偏移量
     * @param readEnd      请求在该层级的结束偏移量
     * @param resultList   结果收集列表
     */
    private static void flattenAndFilter(
            String bucket,
            NavigableMap<Long, Inode.InodeData> currentMap,
            long readOffset,
            long readEnd,
            List<Inode.InodeData> resultList) {

        if (currentMap == null || currentMap.isEmpty()) return;

        // --- 极致优化 1: 利用 TreeMap 定位起始位置 ---
        // 找到第一个可能重叠的 Entry (key <= readOffset)
        Map.Entry<Long, Inode.InodeData> firstEntry = currentMap.floorEntry(readOffset);
        long fromKey = (firstEntry != null) ? firstEntry.getKey() : currentMap.firstKey();

        // 截取所有可能重叠的 Entry [fromKey, readEnd)
        NavigableMap<Long, Inode.InodeData> relevantMap = currentMap.subMap(fromKey, true, readEnd, false);

        for (Map.Entry<Long, Inode.InodeData> entry : relevantMap.entrySet()) {
            long itemStart = entry.getKey();
            Inode.InodeData item = entry.getValue();
            long itemEnd = itemStart + item.size;

            // 过滤掉虽然在 subMap 里但由于 size 过小没够到 readOffset 的前置节点
            if (itemEnd <= readOffset) continue;

            // --- 极致优化 2: 递归处理 ---
            if (item.fileName != null && item.fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(bucket, item.fileName);
                
                MyRocksDB.getChunkFileMetaData(chunkKey).ifPresent(chunkFile -> {
                    // 计算子层级的相对偏移区间
                    // 子层级的 readOffset = max(0, 请求起点 - 该项起始点)
                    long nextReadOffset = Math.max(0L, readOffset - itemStart);
                    // 子层级的 readEnd = 请求终点 - 该项起始点
                    long nextReadEnd = readEnd - itemStart;
                    
                    flattenAndFilter(bucket, chunkFile.getChunkMap(), nextReadOffset, nextReadEnd, resultList);
                });
            } else {
                // --- 极致优化 3: 数据裁剪 ---
                // 计算重叠区间
                long overlapStart = Math.max(itemStart, readOffset);
                long overlapEnd = Math.min(itemEnd, readEnd);

                if (overlapStart < overlapEnd) {
                    Inode.InodeData part = new Inode.InodeData(item);
                    // 更新在物理分片内部的相对偏移
                    part.offset += (overlapStart - itemStart); 
                    part.size = overlapEnd - overlapStart;
                    resultList.add(part);
                }
            }
        }
    }

    public static byte[] readRangeOptimized(String bucket, NavigableMap<Long, Inode.InodeData> rootMap, long offset, int count) throws Exception {
        List<Inode.InodeData> targetSegments = new ArrayList<>();
        long readEnd = offset + count;

        // 将 Inode 的 List 转换为临时的 TreeMap 以统一处理逻辑 (如果 rootList 很短，开销极小)
        // NavigableMap<Long, Inode.InodeData> rootMap = new TreeMap<>();
        // long cur = 0;
        // for (Inode.InodeData data : rootList) {
        //     rootMap.put(cur, data);
        //     cur += data.size;
        // }

        // 1. 执行递归查找与裁剪 (O(log N) 搜索)
        flattenAndFilter(bucket, rootMap, offset, readEnd, targetSegments);

        // 2. 分配结果 Buffer
        byte[] resultBuffer = new byte[count];
        int currentWritePos = 0;

        // 3. 物理读取 (逻辑不变，依然利用全局 Channel)
        for (Inode.InodeData segment : targetSegments) {
            // 如果是空洞文件，直接跳过物理读取（Buffer 默认为 0）
            if (segment.fileName == null || segment.fileName.isEmpty()) {
                currentWritePos += (int) segment.size;
                continue;
            }

            FileMetadata fileMetadata = MyRocksDB.getFileMetaData(segment.fileName)
                    .orElseThrow(() -> new RuntimeException("Physical meta missing: " + segment.fileName));

            readPhysicalSlicesToBuffer(
                fileMetadata.getOffset(),
                fileMetadata.getLen(),
                segment.offset, // 这里已经是计算好的段内偏移
                segment.size,   // 这里已经是裁剪后的长度
                resultBuffer,
                currentWritePos
            );
            
            currentWritePos += (int) segment.size;
        }

        return resultBuffer;
    }

    private static void readPhysicalSlicesToBuffer(List<Long> physOffsets, List<Long> physLens, 
                                                long innerOffset, long innerSize, 
                                                byte[] resultBuffer, int bufferStartPos) throws IOException {
        long remainingToSkip = innerOffset;
        long remainingToRead = innerSize;
        int currentTargetPos = bufferStartPos;

        for (int i = 0; i < physOffsets.size() && remainingToRead > 0; i++) {
            long currentFragLen = physLens.get(i);
            if (remainingToSkip >= currentFragLen) {
                remainingToSkip -= currentFragLen;
                continue;
            }

            long readStartInThisFrag = physOffsets.get(i) + remainingToSkip;
            long availableInThisFrag = currentFragLen - remainingToSkip;
            int actualReadLen = (int) Math.min(availableInThisFrag, remainingToRead);

            ByteBuffer buffer = ByteBuffer.wrap(resultBuffer, currentTargetPos, actualReadLen);
            long pos = readStartInThisFrag;
            while (buffer.hasRemaining()) {
                int n = PhysicalStorageEngine.GLOBAL_CHANNEL.read(buffer, pos);
                if (n == -1) break; 
                pos += n;
            }
            
            currentTargetPos += actualReadLen;
            remainingToSkip = 0;
            remainingToRead -= actualReadLen;
        }
    }

    public static void writeRangeOptimized(String bucket, String object, long offset, int count, byte[] data, Inode inode) {
        LogicalMetadataManager.writeRangeOptimized(bucket, object, offset, count, data, inode);
    }
}
