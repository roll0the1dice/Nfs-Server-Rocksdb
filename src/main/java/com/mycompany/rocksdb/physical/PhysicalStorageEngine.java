package com.mycompany.rocksdb.physical;

import com.mycompany.rocksdb.allocator.JavaAllocator;
import com.mycompany.rocksdb.logical.LogicalMetadataManager;
import com.mycompany.rocksdb.myrocksdb.MyRocksDB;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhysicalStorageEngine {
    private static final Logger log = LoggerFactory.getLogger(PhysicalStorageEngine.class);

    private static final int BLOCK_SIZE = 4096;

    public static class PhysicalWriteResult {
        public final List<Long> offsets;
        public final List<Long> lens;
        public final long totalWritten;

        public PhysicalWriteResult(List<Long> offsets, List<Long> lens, long totalWritten) {
            this.offsets = offsets;
            this.lens = lens;
            this.totalWritten = totalWritten;
        }
    }
    /**
     * 阶段一：纯物理写入
     * 职责：分配逻辑块，执行 GLOBAL_CHANNEL 写入
     */
    public static PhysicalWriteResult writeToPhysicalDevice(byte[] data) throws Exception {
        MyRocksDB db = MyRocksDB.getIntegration(MyRocksDB.INDEX_LUN);
        long count = data.length;
        long numBlocks = (count + BLOCK_SIZE - 1) / BLOCK_SIZE;

        // 1. 分配空间 (由底层 Allocator 管理)
        JavaAllocator.Result[] results = db.allocate(numBlocks);
        if (results == null || results.length == 0) throw new RuntimeException("Disk full");

        List<Long> offsets = new ArrayList<>(results.length);
        List<Long> lens = new ArrayList<>(results.length);
        
        int dataOffset = 0;
        for (JavaAllocator.Result res : results) {
            long physOffset = res.offset * BLOCK_SIZE;
            long physLen = res.size * BLOCK_SIZE;
            int bytesToWrite = (int) Math.min(physLen, count - dataOffset);

            // 2. 物理落盘：使用全局 Channel 零拷贝/原子写入
            ByteBuffer buffer = ByteBuffer.wrap(data, dataOffset, bytesToWrite);
            while (buffer.hasRemaining()) {
                // write(buffer, position) 是线程安全的
                int n = GLOBAL_CHANNEL.write(buffer, physOffset + buffer.position() - dataOffset);
                if (n <= 0) break;
            }

            offsets.add(physOffset);
            lens.add((long) bytesToWrite);
            dataOffset += bytesToWrite;
            if (dataOffset >= count) break;
        }

        return new PhysicalWriteResult(offsets, lens, count);
    }

    // 全局单例 Channel
    public static final FileChannel GLOBAL_CHANNEL;

    static {
        try {
            // 在类加载时打开 (或在 Spring @PostConstruct 中打开)
            Path path = Paths.get(MyRocksDB.FILE_DATA_DEVICE_PATH);
            // 如果是普通文件测试，建议加上 CREATE 选项；如果是裸设备，只需 READ, WRITE
            GLOBAL_CHANNEL = FileChannel.open(path, 
                    StandardOpenOption.READ, 
                    StandardOpenOption.WRITE, 
                    StandardOpenOption.CREATE); // 如果文件不存在则创建 (仅限非裸设备路径)
        } catch (IOException e) {
            log.info("Failed to open data device: {}", e);
            throw new RuntimeException("Failed to open data device", e);
        }
    }
}
