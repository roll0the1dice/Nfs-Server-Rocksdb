package com.mycompany.rocksdb.myrocksdb;

import lombok.extern.log4j.Log4j2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mycompany.rocksdb.allocator.JavaAllocator;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * 文件系统初始化器
 * 使用Java版本的分配器来实现初始化已有文件和恢复分配状态
 *
 */
public class FileSystemInitializer {
    private static final Logger log = LoggerFactory.getLogger(FileSystemInitializer.class);

    private final JavaAllocator allocator;
    private final String dataDir;
    private final long blockSize;
    private final long totalBlocks;

    /**
     * 文件信息结构
     */
    public static class FileInfo {
        public String fileName;
        public long fileSize;
        public long startBlock;
        public long blockCount;
        public long lastModified;

        public FileInfo(String fileName, long fileSize, long startBlock, long blockCount, long lastModified) {
            this.fileName = fileName;
            this.fileSize = fileSize;
            this.startBlock = startBlock;
            this.blockCount = blockCount;
            this.lastModified = lastModified;
        }

        @Override
        public String toString() {
            return String.format("FileInfo{name='%s', size=%d, startBlock=%d, blockCount=%d, modified=%d}",
                    fileName, fileSize, startBlock, blockCount, lastModified);
        }
    }

    /**
     * 构造函数
     * @param dataDir 数据目录
     * @param totalSize 总存储大小（字节）
     * @param blockSize 块大小（字节）
     */
    public FileSystemInitializer(String dataDir, long totalSize, long blockSize) {
        this.dataDir = dataDir;
        this.blockSize = blockSize;
        this.totalBlocks = totalSize / blockSize;

        // 创建Java版本的分配器
        this.allocator = new JavaAllocator(totalBlocks, blockSize);

        log.info("FileSystemInitializer initialized: dataDir={}, totalSize={}, blockSize={}, totalBlocks={}",
                dataDir, totalSize, blockSize, totalBlocks);
    }

    /**
     * 初始化已有文件至逻辑空间
     * 扫描数据目录中的所有文件，将它们占用的空间标记为已分配
     */
    public List<FileInfo> initializeExistingFiles() {
        log.info("开始初始化已有文件...");

        List<FileInfo> fileInfos = new ArrayList<>();
        File dir = new File(dataDir);

        if (!dir.exists() || !dir.isDirectory()) {
            log.warn("数据目录不存在或不是目录: {}", dataDir);
            return fileInfos;
        }

        // 扫描目录中的所有文件
        File[] files = dir.listFiles();
        if (files == null) {
            log.warn("无法读取目录内容: {}", dataDir);
            return fileInfos;
        }

        long currentBlockOffset = 0;

        for (File file : files) {
            if (file.isFile()) {
                try {
                    // 计算文件占用的块数
                    long fileSize = file.length();
                    long blockCount = (fileSize + blockSize - 1) / blockSize; // 向上取整

                    // 检查是否有足够的空间
                    if (currentBlockOffset + blockCount > totalBlocks) {
                        log.error("存储空间不足，无法容纳文件: {}", file.getName());
                        break;
                    }

                    // 标记这些块为已分配
                    allocator.initAllocated(currentBlockOffset, blockCount);

                    // 创建文件信息
                    FileInfo fileInfo = new FileInfo(
                            file.getName(),
                            fileSize,
                            currentBlockOffset,
                            blockCount,
                            file.lastModified()
                    );
                    fileInfos.add(fileInfo);

                    log.info("初始化文件: {}", fileInfo);

                    // 更新块偏移量
                    currentBlockOffset += blockCount;

                } catch (Exception e) {
                    log.error("处理文件时出错: {}", file.getName(), e);
                }
            }
        }

        log.info("已有文件初始化完成，共处理 {} 个文件", fileInfos.size());
        allocator.dumpStatus();

        return fileInfos;
    }

    /**
     * 从RocksDB恢复分配状态
     * 模拟从持久化存储中恢复分配器的状态
     */
    public void restoreAllocationState() {
        log.info("开始从RocksDB恢复分配状态...");

        try {
            // 模拟从RocksDB读取已分配的空间信息
            // 这里我们模拟一些已分配的空间区间
            List<AllocationRange> allocatedRanges = loadAllocatedRangesFromRocksDB();

            // 恢复每个已分配区间
            for (AllocationRange range : allocatedRanges) {
                allocator.initAllocated(range.startBlock, range.blockCount);
                log.debug("恢复已分配区间: startBlock={}, blockCount={}",
                        range.startBlock, range.blockCount);
            }

            log.info("分配状态恢复完成，共恢复 {} 个区间", allocatedRanges.size());
            allocator.dumpStatus();

        } catch (Exception e) {
            log.error("恢复分配状态时出错", e);
        }
    }

    /**
     * 模拟从RocksDB加载已分配区间
     * 在实际实现中，这里会从RocksDB读取真实的分配状态
     */
    private List<AllocationRange> loadAllocatedRangesFromRocksDB() {
        List<AllocationRange> ranges = new ArrayList<>();

        // 模拟一些已分配的空间区间
        // 这些数据在实际情况下会从RocksDB中读取
        ranges.add(new AllocationRange(0, 100));      // 0-99块已分配
        ranges.add(new AllocationRange(200, 150));    // 200-349块已分配
        ranges.add(new AllocationRange(500, 80));     // 500-579块已分配
        ranges.add(new AllocationRange(800, 200));    // 800-999块已分配

        log.info("从RocksDB加载了 {} 个已分配区间", ranges.size());
        return ranges;
    }

    /**
     * 保存分配状态到RocksDB
     * 将当前的分配状态持久化到RocksDB中
     */
    public void saveAllocationStateToRocksDB() {
        log.info("开始保存分配状态到RocksDB...");

        try {
            // 扫描L0位图，找出所有已分配的区间
            List<AllocationRange> allocatedRanges = scanAllocatedRanges();

            // 保存到RocksDB（这里只是模拟）
            saveRangesToRocksDB(allocatedRanges);

            log.info("分配状态保存完成，共保存 {} 个区间", allocatedRanges.size());

        } catch (Exception e) {
            log.error("保存分配状态时出错", e);
        }
    }

    /**
     * 扫描L0位图，找出所有已分配的区间
     */
    private List<AllocationRange> scanAllocatedRanges() {
        List<AllocationRange> ranges = new ArrayList<>();

        // 这里需要访问allocator的内部状态
        // 在实际实现中，allocator应该提供扫描方法
        // 为了演示，我们使用一个简化的实现

        log.info("扫描已分配区间...");

        // 模拟扫描结果
        ranges.add(new AllocationRange(0, 100));
        ranges.add(new AllocationRange(200, 150));
        ranges.add(new AllocationRange(500, 80));

        return ranges;
    }

    /**
     * 模拟保存区间到RocksDB
     */
    private void saveRangesToRocksDB(List<AllocationRange> ranges) {
        log.info("保存 {} 个区间到RocksDB", ranges.size());
        for (AllocationRange range : ranges) {
            log.debug("保存区间: startBlock={}, blockCount={}",
                    range.startBlock, range.blockCount);
        }
    }

    /**
     * 创建新文件并分配空间
     * @param fileName 文件名
     * @param fileSize 文件大小（字节）
     * @return 文件信息
     */
    public FileInfo createFile(String fileName, long fileSize) {
        log.info("创建文件: fileName={}, fileSize={}", fileName, fileSize);

        // 计算需要的块数
        long blockCount = (fileSize + blockSize - 1) / blockSize;

        // 分配空间
        JavaAllocator.Result[] results = allocator.allocate(blockCount);

        if (results.length == 0) {
            log.error("空间不足，无法创建文件: {}", fileName);
            return null;
        }

        // 创建文件信息
        FileInfo fileInfo = new FileInfo(
                fileName,
                fileSize,
                results[0].offset,
                results[0].size,
                System.currentTimeMillis()
        );

        log.info("文件创建成功: {}", fileInfo);
        return fileInfo;
    }

    /**
     * 删除文件并释放空间
     * @param fileInfo 文件信息
     */
    public void deleteFile(FileInfo fileInfo) {
        log.info("删除文件: {}", fileInfo.fileName);

        // 释放空间
        allocator.free(fileInfo.startBlock, fileInfo.blockCount);

        log.info("文件删除完成，释放了 {} 个块", fileInfo.blockCount);
    }

    /**
     * 获取分配器状态
     */
    public void dumpAllocatorStatus() {
        allocator.dumpStatus();
    }

    /**
     * 获取可用空间
     */
    public long getAvailableSpace() {
        return allocator.getAvailable() * blockSize;
    }

    /**
     * 获取总空间
     */
    public long getTotalSpace() {
        return totalBlocks * blockSize;
    }

    /**
     * 已分配区间结构
     */
    private static class AllocationRange {
        public final long startBlock;
        public final long blockCount;

        public AllocationRange(long startBlock, long blockCount) {
            this.startBlock = startBlock;
            this.blockCount = blockCount;
        }

        @Override
        public String toString() {
            return String.format("AllocationRange{startBlock=%d, blockCount=%d}",
                    startBlock, blockCount);
        }
    }
}

