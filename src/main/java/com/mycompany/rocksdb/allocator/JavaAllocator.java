package com.mycompany.rocksdb.allocator;

import lombok.extern.log4j.Log4j2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 纯Java实现的分配器，
 * 实现多层级位图分配算法
 */
public class JavaAllocator {
    private static final Logger log = LoggerFactory.getLogger(JavaAllocator.class);

    /**
     * 代表一个逻辑空间块
     */
    public static class Result {
        /**
         * allocator.allocate生成的offset：申请到的逻辑空间块是整个存储空间的第几个块
         */
        public long offset;

        /**
         * allocator.allocate生成的size：申请到的逻辑空间块的个数
         */
        public long size;

        public Result() {}

        public Result(long offset, long size) {
            this.offset = offset;
            this.size = size;
        }

        @Override
        public String toString() {
            return "Result{offset=" + offset + ", size=" + size + "}";
        }
    }

    // ========== 与JNI版本完全一致的基础常量 ==========
    private static final long SLOT_SIZE = 8;                    // sizeof(slot_t) = 8字节
    private static final long BITS_PER_SLOT = SLOT_SIZE * 8;    // 64位
    private static final long SLOTS_PER_SLOTSET = 8;            // 8个槽位每套
    private static final long SLOTSET_BYTES = SLOT_SIZE * SLOTS_PER_SLOTSET;  // 64字节
    private static final long BITS_PER_SLOTSET = SLOTSET_BYTES * 8;           // 512位

    // ========== 各层级常量（与JNI完全一致） ==========
    // L0层级：每个slot代表64个条目
    private static final long L0_ENTRIES_PER_SLOT = BITS_PER_SLOT;  // 64

    // L1层级：每个条目占2位，每个slot代表32个条目
    private static final long L1_ENTRY_WIDTH = 2;
    private static final long L1_ENTRY_MASK = (1L << L1_ENTRY_WIDTH) - 1;  // 3
    private static final long L1_ENTRY_FULL = 0x00;      // 00: 完全分配
    private static final long L1_ENTRY_PARTIAL = 0x01;   // 01: 部分分配
    private static final long L1_ENTRY_NOT_USED = 0x02;  // 10: 未使用
    private static final long L1_ENTRY_FREE = 0x03;      // 11: 完全空闲
    private static final long L1_ENTRIES_PER_SLOT = BITS_PER_SLOT / L1_ENTRY_WIDTH;  // 32

    // L2层级：每个slot代表64个条目
    private static final long L2_ENTRIES_PER_SLOT = BITS_PER_SLOT;  // 64

    // ========== 核心数据结构 ==========
    private final long totalBlocks;           // 总逻辑块数
    private final long allocUnit;             // 分配单元大小（字节）
    private long availableBlocks;             // 可用逻辑块数

    // L0层级：最细粒度位图，每个位代表一个逻辑块
    private final BitSet l0Bitmap;

    // L1层级：中间粒度位图，每个条目占2位，代表一个slotset（512个逻辑块）
    private final long[] l1Bitmap;
    private final long l1Granularity;         // L1粒度：512个逻辑块

    // L2层级：最粗粒度位图，每个位代表一个L1条目
    private final BitSet l2Bitmap;
    private final long l2Granularity;         // L2粒度：L1粒度 * 32

    // 线程安全
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * 构造函数
     * @param totalBlocks 总逻辑块数
     * @param allocUnit 分配单元大小（字节）
     */
    public JavaAllocator(long totalBlocks, long allocUnit) {
        this.totalBlocks = totalBlocks;
        this.allocUnit = allocUnit;
        this.availableBlocks = totalBlocks;

        // 初始化L0位图
        this.l0Bitmap = new BitSet((int) totalBlocks);

        // 计算L1参数
        this.l1Granularity = BITS_PER_SLOTSET;  // 512个逻辑块
        long l1SlotCount = (totalBlocks + l1Granularity - 1) / l1Granularity;
        this.l1Bitmap = new long[(int) l1SlotCount];

        // 计算L2参数
        this.l2Granularity = l1Granularity * L1_ENTRIES_PER_SLOT;  // 512 * 32 = 16384
        long l2SlotCount = (totalBlocks + l2Granularity - 1) / l2Granularity;
        this.l2Bitmap = new BitSet((int) l2SlotCount);

        // 初始化所有层级为空闲状态
        initializeAsFree();

        log.info("JavaAllocator initialized: totalBlocks={}, allocUnit={}, l1Granularity={}, l2Granularity={}",
                totalBlocks, allocUnit, l1Granularity, l2Granularity);
    }

    /**
     * 初始化所有层级为空闲状态
     */
    private void initializeAsFree() {
        lock.writeLock().lock();
        try {
            // L0：所有位设为false（空闲）
            l0Bitmap.clear();

            // L1：所有条目设为FREE（11）
            for (int i = 0; i < l1Bitmap.length; i++) {
                l1Bitmap[i] = 0xFFFFFFFFFFFFFFFFL;  // 所有2位条目都设为11
            }

            // L2：所有位设为true（表示L1有空闲条目）
            l2Bitmap.set(0, (int) Math.ceil((double) totalBlocks / l2Granularity));

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 初始化已分配的空间（对应JNI的initAllocated方法）
     * @param offset 起始逻辑块索引
     * @param size 逻辑块数量
     */
    public void initAllocated(long offset, long size) {
        lock.writeLock().lock();
        try {
            // 参数验证
            if (offset < 0 || size < 0 || offset + size > totalBlocks) {
                throw new IllegalArgumentException("Invalid parameters: offset=" + offset + ", size=" + size + ", totalBlocks=" + totalBlocks);
            }

            log.debug("initAllocated: offset={}, size={}", offset, size);

            // 标记L0位图
            for (long i = offset; i < offset + size; i++) {
                l0Bitmap.set((int) i);
            }

            // 更新L1位图
            updateL1FromL0(offset, offset + size);

            // 更新L2位图
            updateL2FromL1(offset, offset + size);

            // 更新可用块数
            availableBlocks -= size;

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 分配空间
     * @param numBlocks 需要的逻辑块数
     * @return 分配结果数组
     */
    public Result[] allocate(long numBlocks) {
        lock.writeLock().lock();
        try {
            if (numBlocks <= 0 || numBlocks > availableBlocks) {
                return new Result[0];
            }

            List<Result> results = new ArrayList<>();
            long remaining = numBlocks;
            long allocated = 0;

            // 尝试分配连续空间
            while (remaining > 0 && allocated < numBlocks) {
                Result result = allocateContiguous(remaining);
                if (result.size == 0) {
                    break;  // 无法分配更多空间
                }

                results.add(result);
                allocated += result.size;
                remaining -= result.size;
            }

            return results.toArray(new Result[0]);

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 分配连续空间
     * @param numBlocks 需要的逻辑块数
     * @return 分配结果
     */
    private Result allocateContiguous(long numBlocks) {
        // 简化实现：查找第一个可用的连续空间
        int startPos = l0Bitmap.nextClearBit(0);
        if (startPos == -1 || startPos >= totalBlocks) {
            return new Result(0, 0);  // 没有可用空间
        }

        // 查找连续的空闲块
        int endPos = startPos;
        while (endPos < totalBlocks && !l0Bitmap.get(endPos) && (endPos - startPos + 1) < numBlocks) {
            endPos++;
        }

        long allocatedSize = Math.min(endPos - startPos + 1, numBlocks);

        // 标记为已分配
        for (long i = startPos; i < startPos + allocatedSize; i++) {
            l0Bitmap.set((int) i);
        }

        // 更新L1和L2
        updateL1FromL0(startPos, startPos + allocatedSize);
        updateL2FromL1(startPos, startPos + allocatedSize);

        availableBlocks -= allocatedSize;

        return new Result(startPos, allocatedSize);
    }

    /**
     * 释放空间
     * @param offset 起始逻辑块索引
     * @param size 逻辑块数量
     */
    public void free(long offset, long size) {
        lock.writeLock().lock();
        try {
            if (offset < 0 || size < 0 || offset + size > totalBlocks) {
                throw new IllegalArgumentException("Invalid parameters");
            }

            // 清除L0位图
            for (long i = offset; i < offset + size; i++) {
                l0Bitmap.clear((int) i);
            }

            // 更新L1位图
            updateL1FromL0(offset, offset + size);

            // 更新L2位图
            updateL2FromL1(offset, offset + size);

            // 更新可用块数
            availableBlocks += size;

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 根据L0位图更新L1位图
     */
    private void updateL1FromL0(long startPos, long endPos) {
        long l1StartSlot = startPos / l1Granularity;
        long l1EndSlot = (endPos - 1) / l1Granularity;

        for (long l1Slot = l1StartSlot; l1Slot <= l1EndSlot; l1Slot++) {
            long l0Start = Math.max(startPos, l1Slot * l1Granularity);
            long l0End = Math.min(endPos, (l1Slot + 1) * l1Granularity);

            // 检查这个L1槽位对应的L0区域状态
            boolean allAllocated = true;
            boolean allFree = true;

            for (long pos = l0Start; pos < l0End; pos++) {
                if (l0Bitmap.get((int) pos)) {
                    allFree = false;
                } else {
                    allAllocated = false;
                }
            }

            // 更新L1条目
            long l1EntryIndex = l1Slot % L1_ENTRIES_PER_SLOT;
            long l1SlotIndex = l1Slot / L1_ENTRIES_PER_SLOT;

            if (l1SlotIndex < l1Bitmap.length) {
                long mask = L1_ENTRY_MASK << (l1EntryIndex * L1_ENTRY_WIDTH);
                long value;

                if (allAllocated) {
                    value = L1_ENTRY_FULL;
                } else if (allFree) {
                    value = L1_ENTRY_FREE;
                } else {
                    value = L1_ENTRY_PARTIAL;
                }

                l1Bitmap[(int) l1SlotIndex] = (l1Bitmap[(int) l1SlotIndex] & ~mask) | (value << (l1EntryIndex * L1_ENTRY_WIDTH));
            }
        }
    }

    /**
     * 根据L1位图更新L2位图
     */
    private void updateL2FromL1(long startPos, long endPos) {
        long l2StartSlot = startPos / l2Granularity;
        long l2EndSlot = (endPos - 1) / l2Granularity;

        for (long l2Slot = l2StartSlot; l2Slot <= l2EndSlot; l2Slot++) {
            long l1StartSlot = l2Slot * L1_ENTRIES_PER_SLOT;
            long l1EndSlot = Math.min(l1StartSlot + L1_ENTRIES_PER_SLOT, l1Bitmap.length);

            // 检查这个L2槽位对应的L1区域状态
            boolean hasFree = false;
            boolean hasPartial = false;

            for (long l1Slot = l1StartSlot; l1Slot < l1EndSlot; l1Slot++) {
                long l1EntryIndex = l1Slot % L1_ENTRIES_PER_SLOT;
                long l1SlotIndex = l1Slot / L1_ENTRIES_PER_SLOT;

                if (l1SlotIndex < l1Bitmap.length) {
                    long entry = (l1Bitmap[(int) l1SlotIndex] >> (l1EntryIndex * L1_ENTRY_WIDTH)) & L1_ENTRY_MASK;

                    if (entry == L1_ENTRY_FREE) {
                        hasFree = true;
                    } else if (entry == L1_ENTRY_PARTIAL) {
                        hasPartial = true;
                    }
                }
            }

            // 更新L2位图
            if (hasFree || hasPartial) {
                l2Bitmap.set((int) l2Slot);
            } else {
                l2Bitmap.clear((int) l2Slot);
            }
        }
    }

    /**
     * 获取可用块数
     */
    public long getAvailable() {
        lock.readLock().lock();
        try {
            return availableBlocks;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取总块数
     */
    public long getTotalBlocks() {
        return totalBlocks;
    }

    /**
     * 获取分配单元大小
     */
    public long getAllocUnit() {
        return allocUnit;
    }

    /**
     * 获取L0位图状态
     * @param index 块索引
     * @return 是否已分配
     */
    public boolean isBlockAllocated(int index) {
        lock.readLock().lock();
        try {
            if (index < 0 || index >= totalBlocks) {
                throw new IllegalArgumentException("Invalid block index: " + index);
            }
            return l0Bitmap.get(index);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取L0位图的副本（用于分析）
     * @return L0位图副本
     */
    public BitSet getL0BitmapCopy() {
        lock.readLock().lock();
        try {
            return (BitSet) l0Bitmap.clone();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取L1位图的副本（用于分析）
     * @return L1位图副本
     */
    public long[] getL1BitmapCopy() {
        lock.readLock().lock();
        try {
            return l1Bitmap.clone();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取L2位图的副本（用于分析）
     * @return L2位图副本
     */
    public BitSet getL2BitmapCopy() {
        lock.readLock().lock();
        try {
            return (BitSet) l2Bitmap.clone();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 打印状态信息
     */
    public void dumpStatus() {
        lock.readLock().lock();
        try {
            log.info("=== JavaAllocator Status ===");
            log.info("Total Blocks: {}", totalBlocks);
            log.info("Available Blocks: {}", availableBlocks);
            log.info("Alloc Unit: {} bytes", allocUnit);
            log.info("L1 Granularity: {} blocks", l1Granularity);
            log.info("L2 Granularity: {} blocks", l2Granularity);
            log.info("L0 Bitmap Size: {} bits", totalBlocks);
            log.info("L1 Bitmap Size: {} slots ({} bits)", l1Bitmap.length, l1Bitmap.length * 64);
            log.info("L2 Bitmap Size: {} bits", l2Bitmap.size());
            log.info("L0 Entries per Slot: {}", L0_ENTRIES_PER_SLOT);
            log.info("L1 Entries per Slot: {}", L1_ENTRIES_PER_SLOT);
            log.info("L2 Entries per Slot: {}", L2_ENTRIES_PER_SLOT);
        } finally {
            lock.readLock().unlock();
        }
    }
}
