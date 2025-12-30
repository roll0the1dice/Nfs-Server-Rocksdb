package com.mycompany.rocksdb;

import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// 任务定义
class WriteTask {
    private final byte[] key;
    private final byte[] value;

    public WriteTask(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }
}

// 工作线程
class RocksDBWriter implements Runnable {
    private final RocksDB db;
    private final BlockingQueue<WriteTask> queue;
    private final AtomicInteger processedTasks;
    private static final int BATCH_SIZE = 100;

    public RocksDBWriter(RocksDB db, BlockingQueue<WriteTask> queue, AtomicInteger processedTasks) {
        this.db = db;
        this.queue = queue;
        this.processedTasks = processedTasks;
    }

    @Override
    public void run() {
        try (final WriteBatch batch = new WriteBatch(); final WriteOptions writeOptions = new WriteOptions()) {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    WriteTask task = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (task != null) {
                        batch.put(task.getKey(), task.getValue());
                        if (batch.count() >= BATCH_SIZE) {
                            db.write(writeOptions, batch);
                            processedTasks.addAndGet(batch.count());
                            batch.clear();
                        }
                    } else if (queue.isEmpty()) {
                        // 如果队列为空，则跳出循环
                        break;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (RocksDBException e) {
                    e.printStackTrace();
                }
            }
            // 提交剩余的批处理
            if (batch.count() > 0) {
                db.write(writeOptions, batch);
                processedTasks.addAndGet(batch.count());
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}

public class ConcurrentRocksDBProcessor {

    public static void main(String[] args) throws IOException, InterruptedException {
        String dbPath = Files.createTempDirectory("rocksdb-concurrent-example").toAbsolutePath().toString();

        try (final Options options = new Options().setCreateIfMissing(true);
             final RocksDB db = RocksDB.open(options, dbPath)) {

            final int numThreads = 8;
            final int numTasks = 10000;
            final BlockingQueue<WriteTask> taskQueue = new LinkedBlockingQueue<>(numTasks);
            final AtomicInteger processedTasks = new AtomicInteger(0);

            // 生产者：向队列中添加任务
            for (int i = 0; i < numTasks; i++) {
                String key = "key" + i;
                String value = "value" + i;
                taskQueue.put(new WriteTask(key.getBytes(), value.getBytes()));
            }

            // 消费者：创建线程池处理任务
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            for (int i = 0; i < numThreads; i++) {
                executor.submit(new RocksDBWriter(db, taskQueue, processedTasks));
            }

            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);

            System.out.println("所有任务处理完成，总共处理了 " + processedTasks.get() + " 个任务。");

            // 验证数据
            for (int i = 0; i < numTasks; i++) {
                String key = "key" + i;
                byte[] value = db.get(key.getBytes());
                if (value == null || !new String(value).equals("value" + i)) {
                    System.err.println("数据验证失败，key: " + key);
                }
            }
            System.out.println("数据验证成功！");

        } catch (RocksDBException e) {
            e.printStackTrace();
        } finally {
            // 清理数据库文件
            // FileUtils.deleteDirectory(new File(dbPath));
        }
    }
}