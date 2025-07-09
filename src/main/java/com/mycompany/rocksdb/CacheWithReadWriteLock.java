package com.mycompany.rocksdb;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CacheWithReadWriteLock<K, V> {

    private final Map<K, V> map = new HashMap<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock rLock = rwLock.readLock();
    private final Lock wLock = rwLock.writeLock();

    /**
     * 读取缓存 (使用读锁)
     * @param key
     * @return
     */
    public V get(K key) {
        rLock.lock(); // 获取读锁，其他读线程可以进入，写线程需等待
        try {
            System.out.println(Thread.currentThread().getName() + " 正在读取 key: " + key);
            // 模拟耗时操作
            try { TimeUnit.MILLISECONDS.sleep(100); } catch (InterruptedException e) { e.printStackTrace(); }
            return map.get(key);
        } finally {
            System.out.println(Thread.currentThread().getName() + " 读取完成，释放读锁。");
            rLock.unlock();
        }
    }

    /**
     * 写入缓存 (使用写锁)
     * @param key
     * @param value
     */
    public V put(K key, V value) {
        wLock.lock(); // 获取写锁，阻塞其他所有读写线程
        try {
            System.out.println(Thread.currentThread().getName() + " 正在写入 key: " + key);
            // 模拟耗时操作
            try { TimeUnit.MILLISECONDS.sleep(100); } catch (InterruptedException e) { e.printStackTrace(); }
            return map.put(key, value);
        } finally {
            System.out.println(Thread.currentThread().getName() + " 写入完成，释放写锁。");
            wLock.unlock();
        }
    }

    public static void main(String[] args) {
        CacheWithReadWriteLock<String, String> cache = new CacheWithReadWriteLock<>();

        // 启动一个写线程
        new Thread(() -> {
            cache.put("key-writer", "value-writer");
        }, "写线程").start();

        // 启动多个读线程
        for (int i = 0; i < 5; i++) {
            final int temp = i;
            new Thread(() -> {
                cache.put("key" + temp, "value" + temp);
                cache.get("key" + temp);
            }, "读线程-" + i).start();
        }


    }
}