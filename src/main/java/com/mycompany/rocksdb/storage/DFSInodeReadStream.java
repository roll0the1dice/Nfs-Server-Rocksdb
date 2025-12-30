package com.mycompany.rocksdb.storage;

import com.mycompany.rocksdb.POJO.ChunkFile;
import com.mycompany.rocksdb.POJO.FileMetadata;
import com.mycompany.rocksdb.POJO.Inode;
import com.mycompany.rocksdb.myrocksdb.MyRocksDB;
import com.mycompany.rocksdb.utils.FSUtils;

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
        FSUtils.flattenInodeStructure(inode.getBucket(), inode.getInodeData(), flattenedList);
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
                    byte[] rawData = FSUtils.readSegmentDataFromRocksDB(segment);
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
}