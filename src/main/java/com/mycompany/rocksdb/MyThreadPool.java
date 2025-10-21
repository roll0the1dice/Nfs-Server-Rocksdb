package com.mycompany.rocksdb;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 自定义线程池
 */
public class MyThreadPool {

    // 存放任务的阻塞队列
    private final BlockingQueue<Runnable> taskQueue;

    // 存放工作线程的集合
    private final List<Worker> workers;

    // 核心线程数
    private final int coreSize;

    // 标记线程池是否已关闭
    private volatile boolean isShutdown = false;

    /**
     * 构造函数
     *
     * @param coreSize 核心线程数
     * @param queueCapacity 任务队列容量
     */
    public MyThreadPool(int coreSize, int queueCapacity) {
        this.coreSize = coreSize;
        this.taskQueue = new LinkedBlockingQueue<>(queueCapacity);
        this.workers = new ArrayList<>(coreSize);

        // 创建并启动核心线程
        for (int i = 0; i < coreSize; i++) {
            Worker worker = new Worker("ThreadPool-Worker-" + i);
            workers.add(worker);
            worker.start();
        }
    }

    /**
     * 提交任务到线程池
     *
     * @param task 要执行的任务
     * @return 如果提交成功返回 true，如果线程池已关闭则返回 false
     */
    public boolean execute(Runnable task) {
        if (isShutdown) {
            System.out.println("线程池已关闭，无法提交任务。");
            return false;
        }
        return taskQueue.offer(task);
    }

    /**
     * 关闭线程池
     * 等待所有任务执行完毕后关闭
     */
    public void shutdown() {
        this.isShutdown = true;
        // 中断所有工作线程
        for (Worker worker : workers) {
            worker.interrupt();
        }
    }

    /**
     * 工作线程，负责从任务队列中获取并执行任务
     */
    private final class Worker extends Thread {

        public Worker(String name) {
            super(name);
        }

        @Override
        public void run() {
            // 当线程池未关闭或任务队列不为空时，持续执行
            while (!isShutdown || !taskQueue.isEmpty()) {
                Runnable task;
                try {
                    // 从任务队列中获取任务，如果队列为空则阻塞等待
                    task = taskQueue.take();
                } catch (InterruptedException e) {
                    // 线程被中断，跳出循环
                    System.out.println(Thread.currentThread().getName() + " 被中断。");
                    break;
                }

                if (task != null) {
                    try {
                        System.out.println(Thread.currentThread().getName() + " 正在执行任务...");
                        task.run();
                    } catch (Exception e) {
                        System.err.println("任务执行出错：" + e.getMessage());
                    }
                }
            }
            System.out.println(Thread.currentThread().getName() + " 执行完毕并退出。");
        }
    }

    /**
     * 测试
     */
    public static void main(String[] args) throws InterruptedException {
        // 创建一个拥有 3 个核心线程，任务队列容量为 10 的线程池
        MyThreadPool pool = new MyThreadPool(10, 10);

        // 提交 10 个任务
        for (int i = 0; i < 10; i++) {
            final int taskId = i;
            pool.execute(() -> {
                System.out.println("任务 " + taskId + " 正在被执行。");
                try {
                    // 模拟任务执行耗时
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // 等待一段时间让任务执行
        Thread.sleep(5000);

        // 关闭线程池
        System.out.println("准备关闭线程池...");
        pool.shutdown();
        System.out.println("线程池已关闭。");
    }
}