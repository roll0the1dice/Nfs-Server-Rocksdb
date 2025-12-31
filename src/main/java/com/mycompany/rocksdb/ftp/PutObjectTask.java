package com.mycompany.rocksdb.ftp;

import com.mycompany.rocksdb.POJO.Inode;
import com.mycompany.rocksdb.myrocksdb.MyRocksDB;
import com.mycompany.rocksdb.utils.MetaKeyUtils;
import io.reactivex.subjects.ReplaySubject;
import io.reactivex.subjects.Subject;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import io.vertx.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;

import static com.mycompany.rocksdb.MD5Util.calculateMD5;

public class PutObjectTask {
    private final long offset;
    private final Buffer buffer;
    public final Subject<Boolean> res;
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final String bucket;
    private final long inodeId;
    private final String objPath;

    // 新增：完成回调，用于通知主线程 "我上传完了，释放了占用的名额"
    private Runnable onUploadFinished;

    public PutObjectTask(String bucket, String objPath, long inodeId, long offset) {
        this.offset = offset;
        this.buffer = Buffer.buffer();
        this.res = ReplaySubject.create();
        this.bucket = bucket;
        this.objPath = objPath;
        this.inodeId = inodeId;
    }

    // 设置回调
    public void setOnUploadFinished(Runnable onUploadFinished) {
        this.onUploadFinished = onUploadFinished;
    }

    public void handle(Buffer chunk) {
        if (completed.get()) {
            throw new IllegalStateException("Task completed");
        }
        buffer.appendBuffer(chunk);
    }

    // 提交任务 (关键修改)
    public void complete() {
        if (!completed.compareAndSet(false, true)) return;

        // 获取完整数据
        long dataSize = buffer.length();

        // --- 关键：使用 executeBlocking 将 IO 操作移出 EventLoop 线程 ---
        Vertx.currentContext().owner().executeBlocking(promise -> {
            try {
                // ====================================================
                // 这里放入你提供的原有业务逻辑
                // ====================================================

                String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);
                String requestId = MetaKeyUtils.getRequestId();

                // 1. 构建 InodeData
                Inode.InodeData inodeData = new Inode.InodeData();
                inodeData.offset = this.offset;
                inodeData.size = dataSize;
                // 注意：文件名可能需要根据分片生成，或者由底层 appendData 处理
                inodeData.fileName = MetaKeyUtils.getObjFileName(bucket, objPath, requestId);
                // 这里计算 MD5 可能会比较耗时，放在这里正好
                inodeData.etag = calculateMD5(buffer.getBytes());
                inodeData.storage = "dataa";

                // 2. 获取/保存 Inode
                // 注意：这里需要处理并发更新 Inode 的问题。
                // 如果 MyRocksDB 内部没有锁，多个 Task 同时 save 可能会冲突。
                Optional<Inode> inodeOptional = MyRocksDB.getINodeMetaData(
                        targetVnodeId, bucket, inodeId
                );

                if (inodeOptional.isPresent()) {
                    Inode inode = inodeOptional.get();

                    //InodeWriteUtils.processWriteRequest(inode, this.offset, inodeData, buffer, targetVnodeId);

                    promise.complete(true); // 成功
                } else {
                    promise.fail("Failed to get Inode");
                }

            } catch (Exception e) {
                promise.fail(e);
            } finally {
                triggerFinishedCallback();
            }
        }, resAr -> {
            // 回到 EventLoop 线程处理结果

            // 1. 释放 Buffer 内存 (非常重要，否则 OOM)
            // 如果 buffer 是 DirectBuffer，显式 release；如果是 HeapBuffer，置 null 帮助 GC
            if (buffer.getByteBuf().refCnt() > 0) {
                buffer.getByteBuf().release();
            }

            // 2. 通知结果
            if (resAr.succeeded()) {
                res.onNext(true);
            } else {
                resAr.cause().printStackTrace(); // 打印错误日志
                res.onNext(false);
            }
            res.onComplete();

            // 3. 触发流控回调 (恢复 Socket)
            if (onUploadFinished != null) {
                onUploadFinished.run();
            }
        });
    }

    private void finalizeTask(boolean success) {
        // 1. 释放内存 (关键)
        if (buffer.getByteBuf().refCnt() > 0) {
            buffer.getByteBuf().release();
        }

        // 2. 通知结果
        res.onNext(success);
        res.onComplete();

        // 3. 触发流控回调 (恢复 Socket)
        if (onUploadFinished != null) {
            onUploadFinished.run();
        }
    }

    public void request() { /* ... */ }

    // 触发回调的辅助方法
    private void triggerFinishedCallback() {
        if (this.onUploadFinished != null) {
            this.onUploadFinished.run();
        }
    }

    private void doActualUploadLogic() {
        // 你的实际写入代码
    }
}