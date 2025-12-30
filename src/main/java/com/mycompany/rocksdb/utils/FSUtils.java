package com.mycompany.rocksdb.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import com.mycompany.rocksdb.POJO.ChunkFile;
import com.mycompany.rocksdb.POJO.FileMetadata;
import com.mycompany.rocksdb.POJO.Inode;
import com.mycompany.rocksdb.constant.GlobalConstant;
import com.mycompany.rocksdb.myrocksdb.MyRocksDB;

import static com.mycompany.rocksdb.constant.GlobalConstant.ROCKS_CHUNK_FILE_KEY;

public class FSUtils {
        // 扁平化逻辑 (同之前)
    public static void flattenInodeStructure(String bucket, List<Inode.InodeData> currentList, List<Inode.InodeData> resultList) {
        if (currentList == null) return;
        for (Inode.InodeData item : currentList) {
            if (item.fileName != null && item.fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(bucket, item.fileName);
                ChunkFile chunkFile = MyRocksDB.getChunkFileMetaData(chunkKey).orElse(null);
                if (chunkFile != null) flattenInodeStructure(bucket, chunkFile.getChunkList(), resultList);
            } else {
                resultList.add(item);
            }
        }
    }

    // RocksDB 读取逻辑 (同之前)
    public static byte[] readSegmentDataFromRocksDB(Inode.InodeData segment) throws Exception {
        // 1. 查物理元数据
        FileMetadata fileMetadata = MyRocksDB.getFileMetaData(segment.fileName)
                .orElseThrow(() -> new RuntimeException("Meta missing: " + segment.fileName));

        // 2. 读物理分片
        List<byte[]> fragments = readSegment(fileMetadata.getOffset(), fileMetadata.getLen(), segment.size);

        // 3. 返回数据
        if (fragments.size() == 1) {
            return fragments.get(0);
        } else {
            // 仅在必要时合并
            return mergeByteArrays(fragments, (int) segment.size);
        }
    }

    // 简单的合并工具方法
    private static byte[] mergeByteArrays(List<byte[]> list, int totalSize) {
        byte[] result = new byte[totalSize];
        int offset = 0;
        for (byte[] b : list) {
            if (b != null) {
                System.arraycopy(b, 0, result, offset, b.length);
                offset += b.length;
            }
        }
        return result;
    }

    /**
     * 读取物理分片数据
     * 运行环境：必须在 Worker 线程 (executeBlocking) 中运行，因为这是阻塞 IO
     */
    /**
     * 优化后的读取方法
     * 此时不再有 try-with-resources 关闭 channel，因为它是全局复用的
     */
    public static List<byte[]> readSegment(List<Long> offsets, List<Long> lens, long expectedSize) throws IOException {
        List<byte[]> result = new ArrayList<>(offsets.size());

        // 维护剩余需要读取的字节数
        long remainingSize = expectedSize;

        for (int i = 0; i < offsets.size(); i++) {
            if (remainingSize <= 0) {
                break;
            }

            long offset = offsets.get(i);
            int length = lens.get(i).intValue();

            // 【优化2】计算本次实际需要读取的长度
            // 取 "该分片物理长度" 和 "剩余所需长度" 的较小值
            // 场景：分片长 100，但我只需要读最后 10 字节，这里就会截断
            long bytesToReadThisTime = Math.min(length, remainingSize);

            if (bytesToReadThisTime <= 0) {
                break;
            }

            // 【优化3】直接分配 byte[] 并 wrap，避免 allocate + get 的双重拷贝
            byte[] data = new byte[(int) bytesToReadThisTime];
            ByteBuffer buffer = ByteBuffer.wrap(data);

            long currentReadPos = offset;
            while (buffer.hasRemaining()) {
                // 使用全局 Channel 读取，无需 open/close 开销
                int n = GLOBAL_CHANNEL.read(buffer, currentReadPos);
                if (n == -1) break;
                currentReadPos += n;
            }
            result.add(data);

            remainingSize -= bytesToReadThisTime;
        }
        return result;
    }


    public static List<Inode.InodeData> partialRead(ChunkFile chunkFile, long readOffset, long readSize) {
        List<Inode.InodeData> result = new ArrayList<>();
        long curOffset = 0L;
        long readEnd = readOffset + readSize;

        long calOffset = 0;
        for (Inode.InodeData cur : chunkFile.getChunkList()) {
            //cur.offset = calOffset;
            //calOffset += cur.size;

            long curEnd = curOffset + cur.size;

            // 当前块完全在读取区间左侧，跳过
            if (curEnd <= readOffset) {
                curOffset = curEnd;
                continue;
            }

            // 当前块完全在读取区间右侧，结束
            if (curOffset >= readEnd) {
                break;
            }
//         [2169661,    3145728]
//            [3076096, 3145728]
            // 计算重叠区间
            long overlapStart = Math.max(curOffset, readOffset);
            long overlapEnd = Math.min(curEnd, readEnd);

            if (overlapStart < overlapEnd) {
                // 构造只包含重叠部分的新 InodeData
                Inode.InodeData part = new Inode.InodeData(cur);
                part.offset = overlapStart - curOffset;
                part.size = overlapEnd - overlapStart;
                result.add(part);
            }

            curOffset = curEnd;
        }

        return result;
    }

    /**
     * 读取一个段的特定部分
     * @param segment 这里的 segment.offset 是指在该 Inode 指向的数据块内部的偏移
     */
    public static byte[] readSegmentDataSlice(Inode.InodeData segment) throws Exception {
        // 1. 获取物理元数据
        FileMetadata fileMetadata = MyRocksDB.getFileMetaData(segment.fileName)
                .orElseThrow(() -> new RuntimeException("Meta missing: " + segment.fileName));

        // 2. 调用支持逻辑偏移的物理读取逻辑
        List<byte[]> fragments = readPhysicalSlices(
                fileMetadata.getOffset(), 
                fileMetadata.getLen(), 
                segment.offset, // 段内偏移
                segment.size    // 需要读取的大小
        );

        // 3. 合并
        return mergeByteArrays(fragments, (int) segment.size);
    }


    /**
     * 物理层级：在多个物理碎片中跳过前面的偏移，读取指定长度
     */
    private static List<byte[]> readPhysicalSlices(List<Long> physOffsets, List<Long> physLens, long innerOffset, long innerSize) throws IOException {
        List<byte[]> result = new ArrayList<>();
        long remainingToSkip = innerOffset;
        long remainingToRead = innerSize;

        for (int i = 0; i < physOffsets.size() && remainingToRead > 0; i++) {
            long currentFragLen = physLens.get(i);

            // 场景 A: 还需要跳过数据
            if (remainingToSkip >= currentFragLen) {
                remainingToSkip -= currentFragLen;
                continue;
            }

            // 场景 B: 已经跳够了，开始读取
            long readStartInThisFrag = physOffsets.get(i) + remainingToSkip;
            long availableInThisFrag = currentFragLen - remainingToSkip;
            long actualReadLen = Math.min(availableInThisFrag, remainingToRead);

            // 执行物理读取
            byte[] data = new byte[(int) actualReadLen];
            ByteBuffer buffer = ByteBuffer.wrap(data);
            
            long pos = readStartInThisFrag;
            while (buffer.hasRemaining()) {
                int n = GLOBAL_CHANNEL.read(buffer, pos);
                if (n == -1) break;
                pos += n;
            }
            
            result.add(data);

            // 之后不需要再跳过了
            remainingToSkip = 0;
            remainingToRead -= actualReadLen;
        }
        
        return result;
    }

    /**
     * 从给定的 Inode 列表中读取任意区间的数据
     * @param bucket 存储桶名称
     * @param rootList 初始的 Inode 列表（可能包含 ChunkFile 引用）
     * @param readOffset 全局逻辑偏移量
     * @param readSize 需要读取的长度
     * @return 读取到的字节数组
     */
    public static byte[] readRange(String bucket, List<Inode.InodeData> rootList, long readOffset, long readSize) throws Exception {
        // 1. 扁平化：处理所有嵌套的 ChunkFile，得到完整的逻辑段列表
        List<Inode.InodeData> flattenedList = new ArrayList<>();
        flattenInodeStructure(bucket, rootList, flattenedList);

        // 2. 逻辑切片：利用你现有的 partialRead 逻辑，计算出落在区间内的段及段内偏移
        // 注意：partialRead 内部会根据 readOffset 过滤掉不相关的 InodeData，并修改其 offset 和 size
        // 这里的 chunkFile 只是为了包装 list 传参，或者直接改写 partialRead 逻辑
        ChunkFile wrapper = new ChunkFile();
        wrapper.setChunkList(flattenedList);
        List<Inode.InodeData> targetSegments = partialRead(wrapper, readOffset, readSize);

        // 3. 物理读取：遍历命中的每一个段，从物理设备读取
        byte[] resultBuffer = new byte[(int) readSize];
        int currentWritePos = 0;

        for (Inode.InodeData segment : targetSegments) {
            // segment.fileName 是物理映射键
            // segment.offset 是该段内部的起始偏移
            // segment.size   是该段需要读取的长度
            byte[] data = readSegmentDataSlice(segment);
            
            System.arraycopy(data, 0, resultBuffer, currentWritePos, data.length);
            currentWritePos += data.length;
        }

        return resultBuffer;
    }

    /**
     * 直接将物理分片数据读入预分配的 resultBuffer 中，避免中间 List<byte[]> 的合并开销
     */
    private static void readPhysicalSlicesToBuffer(List<Long> physOffsets, List<Long> physLens, 
                                                long innerOffset, long innerSize, 
                                                byte[] resultBuffer, int bufferStartPos) throws IOException {
        long remainingToSkip = innerOffset;
        long remainingToRead = innerSize;
        int currentTargetPos = bufferStartPos;

        for (int i = 0; i < physOffsets.size() && remainingToRead > 0; i++) {
            long currentFragLen = physLens.get(i);

            // 跳过不属于请求范围的物理碎片
            if (remainingToSkip >= currentFragLen) {
                remainingToSkip -= currentFragLen;
                continue;
            }

            // 计算当前物理碎片的读取起点和长度
            long readStartInThisFrag = physOffsets.get(i) + remainingToSkip;
            long availableInThisFrag = currentFragLen - remainingToSkip;
            int actualReadLen = (int) Math.min(availableInThisFrag, remainingToRead);

            // 使用 ByteBuffer.wrap 直接映射目标数组的特定区间
            // 这样 GLOBAL_CHANNEL.read 会直接把数据写进 resultBuffer
            ByteBuffer buffer = ByteBuffer.wrap(resultBuffer, currentTargetPos, actualReadLen);
            
            long pos = readStartInThisFrag;
            while (buffer.hasRemaining()) {
                int n = GLOBAL_CHANNEL.read(buffer, pos);
                if (n == -1) break; 
                pos += n;
            }
            
            currentTargetPos += actualReadLen;
            remainingToSkip = 0; // 跳过一次后，后续碎片从 0 开始读
            remainingToRead -= actualReadLen;
        }
    }


    /**
     * 合并了递归展开和区间过滤的方法
     * 
     * @param bucket          存储桶
     * @param currentList     当前的 Inode 列表
     * @param readOffset      请求的全局逻辑起始偏移量
     * @param readEnd         请求的全局逻辑结束位置 (readOffset + readSize)
     * @param currentPointer  当前遍历到的全局逻辑位置（用数组模拟引用传递）
     * @param resultList      结果收集列表
     */
    private static void flattenAndFilter(
            String bucket,
            List<Inode.InodeData> currentList,
            long readOffset,
            long readEnd,
            long[] currentPointer,
            List<Inode.InodeData> resultList) {

        if (currentList == null) return;

        for (Inode.InodeData item : currentList) {
            long itemStart = currentPointer[0];
            long itemEnd = itemStart + item.size;

            // --- 优化 1: 范围剪枝 ---
            // 场景 A: 当前项完全在请求区间左侧，跳过并累加偏移
            if (itemEnd <= readOffset) {
                currentPointer[0] += item.size;
                continue;
            }
            // 场景 B: 当前项完全在请求区间右侧，结束当前层级遍历
            if (itemStart >= readEnd) {
                return;
            }

            // --- 优化 2: 递归处理 ---
            if (item.fileName != null && item.fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                // 是一个 ChunkFile 引用
                String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(bucket, item.fileName);
                // 只有当这个 ChunkFile 包含我们需要的数据时，才去查 RocksDB
                MyRocksDB.getChunkFileMetaData(chunkKey).ifPresent(chunkFile -> {
                    flattenAndFilter(bucket, chunkFile.getChunkList(), readOffset, readEnd, currentPointer, resultList);
                });
                // 注意：递归内部会更新 currentPointer，这里不需要再次累加
            } else {
                // 是真实的数据分片 (Physical Segment)
                // 计算当前分片与请求区间的重叠部分
                long overlapStart = Math.max(itemStart, readOffset);
                long overlapEnd = Math.min(itemEnd, readEnd);

                if (overlapStart < overlapEnd) {
                    // 构造裁剪后的分片信息
                    Inode.InodeData part = new Inode.InodeData(item);
                    // 修改逻辑：part.offset 现在代表在该物理分片内部的相对起始偏移
                    part.offset = overlapStart - itemStart;
                    part.size = overlapEnd - overlapStart;
                    resultList.add(part);
                }
                // 更新全局指针
                currentPointer[0] += item.size;
            }
        }
    }

    public static byte[] readRangeOptimized(String bucket, List<Inode.InodeData> rootList, long offset, int count) throws Exception {
        // 1. 准备参数
        List<Inode.InodeData> targetSegments = new ArrayList<>();
        long[] currentPointer = {0L}; // 必须从 0 开始
        long readEnd = offset + count;

        // 2. 执行合并后的“查找+裁剪”逻辑
        flattenAndFilter(bucket, rootList, offset, readEnd, currentPointer, targetSegments);

        // 3. 分配 NFS 适配的 Buffer
        byte[] resultBuffer = new byte[count];
        int currentWritePos = 0;

        // 4. 执行物理读取
        for (Inode.InodeData segment : targetSegments) {
            FileMetadata fileMetadata = MyRocksDB.getFileMetaData(segment.fileName)
                    .orElseThrow(() -> new RuntimeException("Meta missing: " + segment.fileName));

            // 这里的 segment.offset 已经是我们计算好的“分片内相对偏移”
            // segment.size 也是裁剪后的实际需要读取大小
            readPhysicalSlicesToBuffer(
                fileMetadata.getOffset(),
                fileMetadata.getLen(),
                segment.offset,
                segment.size,
                resultBuffer,
                currentWritePos
            );
            
            currentWritePos += (int) segment.size;
        }

        return resultBuffer;
    }
        // 全局单例 Channel
    private static final FileChannel GLOBAL_CHANNEL;

    static {
        try {
            // 在类加载时打开 (或在 Spring @PostConstruct 中打开)
            GLOBAL_CHANNEL = FileChannel.open(Paths.get(MyRocksDB.FILE_DATA_DEVICE_PATH), StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new RuntimeException("Failed to open data device", e);
        }
    }
}
