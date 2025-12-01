package com.mycompany.rocksdb.storage;

import com.mycompany.rocksdb.POJO.ChunkFile;
import com.mycompany.rocksdb.POJO.FileMetadata;
import com.mycompany.rocksdb.POJO.Inode;
import com.mycompany.rocksdb.myrocksdb.MyRocksDB;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static com.mycompany.rocksdb.constant.GlobalConstant.ROCKS_CHUNK_FILE_KEY;

public class DFSInodeReadStream implements ReadStream<Buffer> {

    private final Vertx vertx;
    private final Iterator<Inode.InodeData> segmentIterator;

    // 状态管理
    private boolean paused = false;
    private boolean closed = false;
    private boolean readInProgress = false; // 防止重入读取

    // 回调 Handler
    private Handler<Buffer> dataHandler;
    private Handler<Void> endHandler;
    private Handler<Throwable> exceptionHandler;

    /**
     * 构造函数
     * @param vertx Vertx 实例
     * @param inode 根 Inode
     */
    public DFSInodeReadStream(Vertx vertx, Inode inode) {
        this.vertx = vertx;
        // 1. 在构造时先扁平化元数据 (Flatten)
        // 注意：如果层级特别深，建议把 flatten 也做成异步的，这里为了简化展示设为同步
        List<Inode.InodeData> flattenedList = new LinkedList<>();
        flattenInodeStructure(inode.getBucket(), inode.getInodeData(), flattenedList);
        this.segmentIterator = flattenedList.iterator();
    }

    // --- 核心读取循环 ---

    private void doRead() {
        // 如果 暂停、已关闭、数据处理完、或者正在读取中，则跳过
        if (paused || closed || dataHandler == null) {
            return;
        }
        if (readInProgress) {
            return;
        }

        // 检查是否还有数据
        if (!segmentIterator.hasNext()) {
            if (endHandler != null) {
                endHandler.handle(null);
            }
            return;
        }

        // 标记正在读取，防止并发调用
        readInProgress = true;

        // 获取下一个任务
        Inode.InodeData segment = segmentIterator.next();

        // 异步读取 (Worker Thread)
        vertx.executeBlocking(promise -> {
            try {
                Buffer buffer;
                // A. 空洞处理
                if (StringUtils.isBlank(segment.fileName)) {
                    buffer = Buffer.buffer(new byte[(int) segment.size]);
                }
                // B. 实际数据处理 (零拷贝封装)
                else {
                    byte[] rawData = readSegmentDataFromRocksDB(segment);
                    buffer = Buffer.buffer(rawData);
                }
                promise.complete(buffer);
            } catch (Exception e) {
                promise.fail(e);
            }
        }, res -> {
            // 回到 EventLoop
            readInProgress = false;

            if (res.succeeded()) {
                Buffer chunk = (Buffer) res.result();
                // 1. 发送数据给消费者 (NetSocket)
                if (dataHandler != null) {
                    dataHandler.handle(chunk);
                }

                // 2. 【关键递归】如果未被暂停，继续读下一块
                if (!paused) {
                    doRead();
                }
            } else {
                if (exceptionHandler != null) {
                    exceptionHandler.handle(res.cause());
                }
            }
        });
    }

    // --- ReadStream 接口实现 ---

    @Override
    public ReadStream<Buffer> handler(Handler<Buffer> handler) {
        this.dataHandler = handler;
        // 设置好 handler 后，立即尝试开始读取
        doRead();
        return this;
    }

    @Override
    public ReadStream<Buffer> pause() {
        this.paused = true;
        return this;
    }

    @Override
    public ReadStream<Buffer> resume() {
        if (this.paused) {
            this.paused = false;
            // 恢复后，触发读取循环
            doRead();
        }
        return this;
    }

    @Override
    public ReadStream<Buffer> endHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
        return this;
    }

    @Override
    public ReadStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
        this.exceptionHandler = handler;
        return this;
    }

    // --- 辅助方法 ---

    @Override
    public ReadStream<Buffer> fetch(long amount) {
        // Vert.x 3.x/4.x 中，fetch 通常用于按需请求，简单实现可以直接调用 resume
        return resume();
    }

    // 扁平化逻辑 (同之前)
    private void flattenInodeStructure(String bucket, List<Inode.InodeData> currentList, List<Inode.InodeData> resultList) {
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
    private byte[] readSegmentDataFromRocksDB(Inode.InodeData segment) throws Exception {
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
    private byte[] mergeByteArrays(List<byte[]> list, int totalSize) {
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