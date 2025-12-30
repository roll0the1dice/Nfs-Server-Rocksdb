package com.mycompany.rocksdb.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mycompany.rocksdb.POJO.ChunkFile;
import com.mycompany.rocksdb.POJO.Inode;
import com.mycompany.rocksdb.myrocksdb.MyRocksDB;
import com.mycompany.rocksdb.utils.MetaKeyUtils;
import io.vertx.reactivex.core.buffer.Buffer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

import static com.mycompany.rocksdb.utils.MetaKeyUtils.getRequestId;

/**
 * Inode 数据写入与元数据更新工具类
 */
@Slf4j
public class InodeWriteUtils {

    /**
     * 处理 Inode 的数据写入逻辑（追加或重叠写）
     *
     * @param inode         当前的 Inode 对象
     * @param reqOffset     写入请求的偏移量
     * @param newInodeData  新构建的 InodeData 元数据对象
     * @param receivedData  接收到的实际数据对象 (包含 .getBytes())
     * @param targetVnodeId 目标 Vnode ID
     */
    public static void processWriteRequest(Inode inode,
                                           long reqOffset,
                                           Inode.InodeData newInodeData,
                                           Buffer receivedData, // 假设的接收数据类型
                                           String targetVnodeId) {

        List<Inode.InodeData> dataList = inode.getInodeData();
        long currentFileSize = inode.getSize();
        byte[] dataBytes = receivedData.getBytes();

        // -------------------------------------------------------
        // 场景 1: 追加写入 (Append) - 偏移量在文件末尾或之后
        // -------------------------------------------------------
        if (reqOffset >= currentFileSize) {
            // 如果偏移量大于当前大小，说明中间有空洞 (Hole)
            if (reqOffset > currentFileSize) {
                long holeSize = reqOffset - currentFileSize;
                // 插入空洞数据块
                 InodeWriteUtils.appendData(dataList, Inode.InodeData.newHoleFile(holeSize), inode, null);
            }
            // 追加实际数据块
            InodeWriteUtils.appendData(dataList, newInodeData, inode, dataBytes);
        }
        // -------------------------------------------------------
        // 场景 2: 重叠写入 / 尾部修改 (Overlap / Partial Overwrite)
        // -------------------------------------------------------
        else {
            handlePartialOverwrite(inode, reqOffset, newInodeData, dataBytes, targetVnodeId);
        }
    }

    /**
     * 处理部分覆盖/重叠写的私有方法
     */
    private static void handlePartialOverwrite(Inode inode,
                                               long reqOffset,
                                               Inode.InodeData newInodeData,
                                               byte[] dataBytes,
                                               String targetVnodeId) {
        List<Inode.InodeData> dataList = inode.getInodeData();

        // 校验：防止列表为空时进行重叠写操作
        if (dataList.isEmpty()) {
            throw new IllegalStateException("Data inconsistency: Offset < Size but InodeData list is empty.");
        }

        // 获取列表最后一个数据块
        // 注意：此逻辑假设重叠写只发生在最后一个块。如果支持随机写（修改文件中间），此处需要根据 reqOffset 遍历查找对应的 Block。
        Inode.InodeData lastInodeData = dataList.get(dataList.size() - 1);

        // 1. 获取 ChunkFile 元数据
        String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(inode.getBucket(), lastInodeData.fileName);
        ChunkFile chunkFile = MyRocksDB.getChunkFileMetaData(chunkKey)
                .orElseThrow(() -> new RuntimeException("Chunk file not found for key: " + chunkKey));

        // 2. 执行覆盖逻辑，并获取文件大小的"增量" (Delta)
        // 假设 partialOverwrite3 返回的是此次操作增加的字节数
        long sizeIncrement = InodeWriteUtils.partialOverwrite(chunkFile, reqOffset, newInodeData);

        // 3. 保存新数据的物理存储元数据
        String versionKey = MetaKeyUtils.getVersionMetaDataKey(targetVnodeId, inode.getBucket(), inode.getObjName(), null);
        // 这里假设 fileSize 参数为本次写入数据的长度
        MyRocksDB.saveFileMetaData(newInodeData.fileName, versionKey, dataBytes, dataBytes.length, true);

        // 4. 更新内存对象状态
        long newChunkSize = chunkFile.getSize() + sizeIncrement;

        chunkFile.setSize(newChunkSize);

        // [核心修复] 更新 Inode 总大小时，应该是：旧总大小 + 增量
        // 原代码逻辑可能有误：inode.setSize(chunkFile.getSize() + sizeIncrement);
        inode.setSize(inode.getSize() + sizeIncrement);

        // 更新 InodeData 引用
        lastInodeData.setChunkNum(chunkFile.getChunkList().size());
        lastInodeData.setSize(newChunkSize);

        // 5. 持久化元数据
        // 建议：如果底层支持，应将下面两行放入同一个 WriteBatch 中以保证原子性
        MyRocksDB.saveINodeMetaData(targetVnodeId, inode);
        MyRocksDB.saveChunkFileMetaData(chunkKey, chunkFile);
    }

    /**
     * 追加数据到 Inode
     *
     * @param list        Inode 中的数据列表
     * @param newSegment  本次要追加的新数据片段元数据
     * @param inode       当前文件的 Inode
     * @param dataBytes   实际的二进制数据 (可以为 null，表示空洞)
     */
    public static void appendData(List<Inode.InodeData> list,
                                  Inode.InodeData newSegment,
                                  Inode inode,
                                  byte[] dataBytes) {

        // 场景 1: Inode 还没有数据，初始化创建
        if (list.isEmpty()) {
            if (StringUtils.isBlank(newSegment.fileName)) {
                createHole(list, newSegment, inode);
            } else {
                createData(list, newSegment, inode, dataBytes);
            }
            return; // 处理完毕直接返回
        }

        // 场景 2: 追加到现有的 Chunk (Last Chunk)
        // 获取 Inode 中记录的最后一个 Chunk 引用
        Inode.InodeData lastChunkRef = list.get(list.size() - 1);

        // 1. 获取物理 ChunkFile 元数据
        String bucket = inode.getBucket();
        String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);
        String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(bucket, lastChunkRef.fileName);

        ChunkFile chunkFile = MyRocksDB.getChunkFileMetaData(chunkKey)
                .orElseThrow(() -> new RuntimeException("Chunk file metadata not found for key: " + chunkKey));

        // 2. 先执行物理数据写入 (Data Write)
        // 这样做的好处是：如果写数据失败（如磁盘满），元数据还未修改，文件系统保持一致状态（旧状态）

        if (StringUtils.isNotBlank(newSegment.fileName) && dataBytes != null && dataBytes.length > 0) {
            // 修正拼写 verisonKey -> versionKey
            String versionKey = MetaKeyUtils.getVersionMetaDataKey(targetVnodeId, bucket, inode.getObjName(), null);
            // 写入实际数据块
            MyRocksDB.saveFileMetaData(newSegment.fileName, versionKey, dataBytes, dataBytes.length, true);
        }

        // 3. 更新内存中的元数据对象状态 (Memory Update)
        // 将新的分片信息加入 ChunkFile 的列表
        chunkFile.getChunkList().add(newSegment);

        long segmentSize = newSegment.getSize();

        // 更新 ChunkFile 的总大小
        long newChunkTotalSize = chunkFile.getSize() + segmentSize;
        chunkFile.setSize(newChunkTotalSize);

        // 更新 Inode 的总大小
        inode.setSize(inode.getSize() + segmentSize);

        // 更新 Inode 中对该 Chunk 的引用信息 (大小和包含的分片数)
        lastChunkRef.setSize(newChunkTotalSize);
        lastChunkRef.setChunkNum(chunkFile.getChunkList().size());

        // 4. 持久化元数据 (Metadata Persistence)
        // 此时数据已落盘，内存对象已更新，最后一步原子化保存元数据
        // 建议：如果 MyRocksDB 支持 WriteBatch，将下面两行合并为一个 Batch
        MyRocksDB.saveINodeMetaData(targetVnodeId, inode);
        MyRocksDB.saveChunkFileMetaData(chunkKey, chunkFile);
    }

    /**
     * 创建空洞块 (Hole Chunk)
     *
     * @param list       Inode 的 Chunk 引用列表
     * @param holeData   空洞的数据片段信息 (包含大小等)
     * @param inode      文件 Inode
     */
    public static void createHole(List<Inode.InodeData> list,
                                  Inode.InodeData holeData,
                                  Inode inode) {        // 1. 生成空洞的唯一标识和 Key
        // 假设 getRequestId() 是当前上下文获取 ID 的方法，建议作为参数传入以减少隐式依赖
        String holeFileName = MetaKeyUtils.getHoleFileName(inode.getBucket(), getRequestId());


        // 2. 构建 ChunkFile 元数据对象 (实体)
        // 直接在 Builder 中初始化 List 和 Size，减少后续 setter 调用
        List<Inode.InodeData> chunkSegmentList = new ArrayList<>();
        chunkSegmentList.add(holeData);

        ChunkFile chunkFile = ChunkFile.builder()
                .nodeId(inode.getNodeId())
                .bucket(inode.getBucket())
                .objName(inode.getObjName())
                .versionId(inode.getVersionId())
                .versionNum(MetaKeyUtils.getVersionNum())
                .size(holeData.size)         // 直接设置大小
                .chunkList(chunkSegmentList) // 直接装入数据
                .hasDeleteFiles(new LinkedList<>())
                .build();

        // 3. 构建 Inode 内部的引用对象 (Reference)
        // 这里的 'chunkRef' 是存放在 Inode.getInodeData() 列表里的轻量级对象
        Inode.InodeData chunkRef = ChunkFile.newChunk(holeFileName, holeData, inode);
        list.add(chunkRef);

        // 4. 更新 Inode 总大小
        // [Bug修复] 必须是 +=，不能是 =，否则会覆盖原有文件大小
        inode.setSize(inode.getSize() + holeData.size);

        // 5. 持久化元数据 (调整顺序)
        String targetVnodeId = MetaKeyUtils.getTargetVnodeId(inode.getBucket());

        // Step A: 先保存 ChunkFile (子节点)
        // 这样即使失败，Inode 还没更新，不会指向一个不存在的 Chunk
        String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(inode.getBucket(), chunkRef.fileName);
        MyRocksDB.saveChunkFileMetaData(chunkKey, chunkFile);

        // Step B: 后保存 Inode (父节点)
        MyRocksDB.saveINodeMetaData(targetVnodeId, inode);
    }

    /**
     * 创建新的数据 Chunk 并写入物理数据
     *
     * @param list        Inode 的 Chunk 引用列表
     * @param segmentData 新的数据分片元数据
     * @param inode       文件 Inode
     * @param dataBytes   实际的二进制数据
     */
    public static void createData(List<Inode.InodeData> list,
                                  Inode.InodeData segmentData,
                                  Inode inode,
                                  byte[] dataBytes) {

        String bucket = inode.getBucket();
        String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);

        // ---------------------------------------------------------
        // Step 1: 先持久化物理数据 (Data First)
        // ---------------------------------------------------------
        // 这是一个关键的 Crash Consistency 保护。
        // 如果这里失败，元数据还未修改，文件系统状态依然是健康的（只是写入失败，文件没坏）。
        if (dataBytes != null && dataBytes.length > 0) {
            // 修正变量名 verisonKey -> versionKey
            String versionKey = MetaKeyUtils.getVersionMetaDataKey(targetVnodeId, bucket, inode.getObjName(), null);
            MyRocksDB.saveFileMetaData(segmentData.fileName, versionKey, dataBytes, dataBytes.length, true);
        }

        // ---------------------------------------------------------
        // Step 2: 构建内存对象
        // ---------------------------------------------------------

        // 2. 构建 ChunkFile 实体
        List<Inode.InodeData> newChunkList = new ArrayList<>();
        newChunkList.add(segmentData);

        ChunkFile chunkFile = ChunkFile.builder()
                .nodeId(inode.getNodeId())
                .bucket(bucket)
                .objName(inode.getObjName())
                .versionId(inode.getVersionId())
                .versionNum(MetaKeyUtils.getVersionNum())
                .size(segmentData.size)    // 直接设置大小
                .chunkList(newChunkList)   // 直接装入数据
                .hasDeleteFiles(new LinkedList<>())
                .build();

        // 3. 构建 Inode 内部的引用 (Reference) 并更新列表
        // 将 segmentData 包装成 Chunk 引用
        Inode.InodeData chunkRef = ChunkFile.newChunk(segmentData.fileName, segmentData, inode);
        list.add(chunkRef);

        // 4. 更新 Inode 总大小
        // [Bug修复] 必须累加，不能覆盖
        inode.setSize(inode.getSize() + segmentData.size);

        // ---------------------------------------------------------
        // Step 3: 持久化元数据 (Metadata Last)
        // ---------------------------------------------------------
        // 顺序原则：先保存子节点(Chunk)，再保存父节点(Inode)

        // A. 保存 ChunkFile 元数据
        // 注意：请确保 getChunkKey 的生成逻辑与 createHole 中的逻辑保持命名空间的一致性
        String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(bucket, chunkRef.fileName);
        MyRocksDB.saveChunkFileMetaData(chunkKey, chunkFile);

        // B. 保存 Inode 元数据
        MyRocksDB.saveINodeMetaData(targetVnodeId, inode);
    }

    /**
     * 执行部分覆盖写逻辑
     *
     * @param chunkFile   Chunk文件元数据
     * @param coverOffset 写入的起始逻辑偏移量 (Logical Offset)
     * @param newData     新的数据片段元数据
     * @return            文件总大小的增量 (Delta Size)。
     *                    (注意：调用者应使用 inode.setSize(inode.getSize() + delta))
     */
    public static long partialOverwrite(ChunkFile chunkFile, long coverOffset, Inode.InodeData newData) {
        List<Inode.InodeData> oldList = chunkFile.getChunkList();
        List<Inode.InodeData> newList = new ArrayList<>(oldList.size() + 2); // 预估容量

        long currentLogicalOffset = 0;
        long coverEnd = coverOffset + newData.getSize(); // 覆盖结束位置

        long totalOverwrittenSize = 0; // 记录被覆盖掉的旧数据大小
        boolean newDataInserted = false;

        for (Inode.InodeData chunk : oldList) {
            long chunkStart = currentLogicalOffset;
            long chunkEnd = chunkStart + chunk.getSize();

            // -------------------------------------------------
            // 场景 1: Chunk 完全在覆盖区左侧 (保留)
            // [Chunk] ... [Overwrite]
            // -------------------------------------------------
            if (chunkEnd <= coverOffset) {
                newList.add(chunk);
            }
            // -------------------------------------------------
            // 场景 2: Chunk 完全在覆盖区右侧 (保留)
            // [Overwrite] ... [Chunk]
            // -------------------------------------------------
            else if (chunkStart >= coverEnd) {
                // 如果还没插入新数据，现在插入 (因为已经到了新数据之后的部分)
                if (!newDataInserted) {
                    newList.add(newData);
                    newDataInserted = true;
                }
                newList.add(chunk);
            }
            // -------------------------------------------------
            // 场景 3: 发生重叠 (Overlap)
            // -------------------------------------------------
            else {
                // 计算重叠部分的长度
                long overlapStart = Math.max(chunkStart, coverOffset);
                long overlapEnd = Math.min(chunkEnd, coverEnd);
                totalOverwrittenSize += (overlapEnd - overlapStart);

                // 3.1 处理左侧剩余部分 (Head Truncation)
                // Chunk: [Start ... CoverOffset ... End]
                if (chunkStart < coverOffset) {
                    // 需要深拷贝，因为我们不修改原对象，防止引用副作用
                    Inode.InodeData leftPart = chunk.clone();
                    // 物理偏移量不变，只需修改大小
                    leftPart.setSize(coverOffset - chunkStart);
                    newList.add(leftPart);
                }

                // 3.2 插入新数据 (如果还没插入)
                if (!newDataInserted) {
                    newList.add(newData);
                    newDataInserted = true;
                }

                // 3.3 处理右侧剩余部分 (Tail Truncation)
                // Chunk: [Start ... CoverEnd ... End]
                if (chunkEnd > coverEnd) {
                    Inode.InodeData rightPart = chunk.clone();
                    // 关键: 右侧剩余部分的物理偏移量需要向后移动
                    // 移动量 = (逻辑覆盖结束点 - 逻辑块起始点)
                    long cutOffSize = coverEnd - chunkStart;
                    rightPart.setOffset(rightPart.getOffset() + cutOffSize);
                    rightPart.setSize(chunkEnd - coverEnd);
                    newList.add(rightPart);
                }

                // 3.4 检查是否完全被覆盖 (Full Overwrite)
                // 如果旧块完全在覆盖范围内，它就被丢弃了。
                // 此时需要将其加入待删除列表(GC用)
                if (chunkStart >= coverOffset && chunkEnd <= coverEnd) {
                    // 只有当有文件名时才需要 GC (空洞没有文件名)
                    if (chunk.fileName != null) {
                        chunkFile.getHasDeleteFiles().add(chunk.fileName);
                    }
                }
            }

            // 移动当前逻辑偏移量指针
            currentLogicalOffset += chunk.getSize();
        }

        // 场景 4: 如果遍历完了还没插入 (例如追加写，或者原文件为空)
        if (!newDataInserted) {
            newList.add(newData);
        }

        // 替换旧列表
        chunkFile.setChunkList(newList);

        // 返回文件大小的变化量 (Delta)
        // Delta = 新数据大小 - 被覆盖掉的旧数据大小
        // 示例: 原文件100B，在 offset 0 写入 10B。Overwritten=10, Delta=0。总大小不变。
        // 示例: 原文件100B，在 offset 90 写入 20B。Overwritten=10 (90-100), Delta=20-10=10。总大小110。
        return newData.getSize() - totalOverwrittenSize;
    }

    public static long partialOverwriteInPlace(ChunkFile chunkFile, long coverOffset, Inode.InodeData newData) {
        List<Inode.InodeData> list = chunkFile.getChunkList();
        ListIterator<Inode.InodeData> it = list.listIterator();

        long currentLogicalOffset = 0;
        long coverEnd = coverOffset + newData.getSize();
        long totalOverwrittenSize = 0;
        boolean dataInserted = false;

        while (it.hasNext()) {
            Inode.InodeData cur = it.next();
            long chunkStart = currentLogicalOffset;
            long chunkEnd = chunkStart + cur.getSize();

            // 1. 完全在左边，无交集 -> 跳过
            if (chunkEnd <= coverOffset) {
                currentLogicalOffset += cur.getSize();
                continue;
            }

            // 2. 完全在右边，无交集 -> 插入并结束
            if (chunkStart >= coverEnd) {
                if (!dataInserted) {
                    it.previous(); // 回退到 cur 之前
                    it.add(newData); // 插入 (注意：这里依然会有 ArrayList 的数组移动性能损耗)
                    it.next(); // 恢复指针指向 cur
                    dataInserted = true;
                }
                break; // 剩下的都不用看了
            }

            // --- 此时必有交集 ---

            // 计算被覆盖的大小
            long overlapStart = Math.max(chunkStart, coverOffset);
            long overlapEnd = Math.min(chunkEnd, coverEnd);
            totalOverwrittenSize += (overlapEnd - overlapStart);

            // 3. 情况 A: 旧块包含新块 (Split into 3: Left, New, Right)
            // [OldStart ... CoverStart ... CoverEnd ... OldEnd]
            if (chunkStart < coverOffset && chunkEnd > coverEnd) {
                // 保存右半部分的属性，稍后插入
                Inode.InodeData rightPart = cur.clone();
                rightPart.setOffset(rightPart.getOffset() + (coverEnd - chunkStart));
                rightPart.setSize(chunkEnd - coverEnd);

                // 修改当前节点为左半部分
                cur.setSize(coverOffset - chunkStart);

                // 插入中间和右边
                it.add(newData);   // 插入新数据
                it.add(rightPart); // 插入右半部分

                dataInserted = true;
                break; // 这种情况下处理完就结束了
            }

            // 4. 情况 B: 只有左边重叠 (修改当前块大小)
            // [OldStart ... CoverStart ... OldEnd] ...
            if (chunkStart < coverOffset && chunkEnd <= coverEnd) {
                cur.setSize(coverOffset - chunkStart);
                // 新数据还没法插入，因为可能跨越了下一个块，继续循环
            }

            // 5. 情况 C: 只有右边重叠 (截断并位移)
            // ... [OldStart ... CoverEnd ... OldEnd]
            else if (chunkStart >= coverOffset && chunkEnd > coverEnd) {
                if (!dataInserted) {
                    it.previous();
                    it.add(newData);
                    it.next();
                    dataInserted = true;
                }
                // 修改当前块为右半部分
                long cutOff = coverEnd - chunkStart;
                cur.setOffset(cur.getOffset() + cutOff);
                cur.setSize(chunkEnd - coverEnd);
            }

            // 6. 情况 D: 完全被覆盖 (删除)
            // [CoverStart ... OldStart ... OldEnd ... CoverEnd]
            else if (chunkStart >= coverOffset && chunkEnd <= coverEnd) {
                if (cur.fileName != null) {
                    chunkFile.getHasDeleteFiles().add(cur.fileName);
                }
                it.remove(); // 移除当前节点
                // remove 后，next() 指针会自动调整，不需要额外操作
                // 但总长度变了，currentLogicalOffset 不需要加 cur.size
                continue;
            }

            currentLogicalOffset += cur.getSize();
        }

        if (!dataInserted) {
            list.add(newData);
        }

        return newData.getSize() - totalOverwrittenSize;
    }
}