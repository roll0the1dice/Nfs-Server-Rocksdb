package com.mycompany.rocksdb;

import lombok.Data;

import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

@Data
public class FreeListSpaceManager {
    // 使用 TreeSet 按 startOffset 存储所有空闲区间
    private final TreeSet<FreeInterval> freeIntervals;

    // 我们需要一个外部变量来追踪逻辑空间的总大小或上限
    // 简单起见，我们假设空间可以无限向后扩展
    private long highWaterMark; // 已分配空间的最高水位线

    public FreeListSpaceManager(long initialStartOffset) {
        this.freeIntervals = new TreeSet<>();
        // 初始时，整个空间都是一个巨大的空闲块
        // 为了简化，我们不预设一个无限大的块，而是在需要时从 highWaterMark 扩展
        this.highWaterMark = initialStartOffset;
    }


//    AtomicLong off = new AtomicLong(0L);
//    public Optional<Long> allocate(long length) {
//        return off.getAndAdd(length);
//    }
//
    /**
     * 分配一个指定长度的逻辑空间。
     * @param length 需要分配的长度
     * @return 成功分配的块的起始偏移，如果失败则返回 Optional.empty()
     */
    public Optional<Long> allocate(long length) {
        if (length <= 0) {
            throw new IllegalArgumentException("Length must be positive.");
        }

        FreeInterval suitableBlock = null;

        // 1. 遍历空闲列表，寻找第一个足够大的块 (First-Fit)
        for (FreeInterval block : freeIntervals) {
            if (block.getLength() >= length) {
                suitableBlock = block;
                break;
            }
        }

        if (suitableBlock != null) {
            // 2. 找到了合适的空闲块
            long allocatedOffset = suitableBlock.getStartOffset();

            // 从空闲列表中移除这个块，因为它将被修改或完全消耗
            freeIntervals.remove(suitableBlock);

            if (suitableBlock.getLength() > length) {
                // 如果块比需要的大，切分它，把剩余部分放回空闲列表
                long newStart = suitableBlock.getStartOffset() + length;
                long newLength = suitableBlock.getLength() - length;
                freeIntervals.add(new FreeInterval(newStart, newLength));
            }

            // 更新最高水位线
            this.highWaterMark = Math.max(this.highWaterMark, allocatedOffset + length);
            return Optional.of(allocatedOffset);
        } else {
            // 3. 没找到合适的空闲块，从末尾分配 (扩展空间)
            long allocatedOffset = this.highWaterMark;
            this.highWaterMark += length;
            return Optional.of(allocatedOffset);
        }
    }

    // 辅助方法
    public void printState() {
        System.out.println("   State: highWaterMark=" + highWaterMark + ", freeIntervals=" + freeIntervals);
    }

    /**
     * 释放一个之前分配的块，将其加回空闲列表并尝试合并。
     * @param offset 要释放的块的起始偏移
     * @param length 要释放的块的长度
     */
    public void deallocate(long offset, long length) {
        if (length <= 0) {
            throw new IllegalArgumentException("Length must be positive.");
        }

        FreeInterval newFreeBlock = new FreeInterval(offset, length);

        // --- 关键的合并逻辑 ---

        // 1. 尝试与前面的块合并
        // 查找刚好在 newFreeBlock 之前的空闲块
        FreeInterval predecessor = freeIntervals.lower(newFreeBlock);
        if (predecessor != null && predecessor.getEndOffset() == newFreeBlock.getStartOffset()) {
            // 它们是相邻的，可以合并！
            freeIntervals.remove(predecessor); // 先移除旧的

            // 扩展 newFreeBlock 来包含前一个块
            newFreeBlock.setStartOffset(predecessor.getStartOffset());
            newFreeBlock.setLength(newFreeBlock.getLength() + predecessor.getLength());
        }

        // 2. 尝试与后面的块合并
        // 查找刚好在 newFreeBlock 之后的空闲块
        FreeInterval successor = freeIntervals.higher(newFreeBlock);
        if (successor != null && newFreeBlock.getEndOffset() == successor.getStartOffset()) {
            // 它们也是相邻的！
            freeIntervals.remove(successor); // 移除旧的

            // 扩展 newFreeBlock 来包含后一个块
            newFreeBlock.setLength(newFreeBlock.getLength() + successor.getLength());
        }

        // 3. 将最终（可能已合并的）新空闲块加入列表
        freeIntervals.add(newFreeBlock);
    }

    public static void main(String[] args) {
        FreeListSpaceManager manager = new FreeListSpaceManager(1000);

        System.out.println("1. Allocating 100 bytes...");
        long offset1 = manager.allocate(100).get();
        System.out.println("   Allocated at: " + offset1); // 1000
        manager.printState(); // highWaterMark=1100, free=[]

        System.out.println("\n2. Allocating 50 bytes...");
        long offset2 = manager.allocate(50).get();
        System.out.println("   Allocated at: " + offset2); // 1100
        manager.printState(); // highWaterMark=1150, free=[]

        System.out.println("\n3. Deallocating block at 1000, length 100...");
        manager.deallocate(1000, 100);
        manager.printState(); // highWaterMark=1150, free=[Free[1000, 1100) len=100]

        System.out.println("\n4. Allocating 30 bytes...");
        long offset3 = manager.allocate(30).get();
        // 找到了 [1000, 1100) 这个空闲块，从中切分
        System.out.println("   Allocated at: " + offset3); // 1000
        // 剩余的 [1030, 1100) 被放回空闲列表
        manager.printState(); // highWaterMark=1150, free=[Free[1030, 1100) len=70]

        System.out.println("\n5. Deallocating block at 1100, length 50...");
        manager.deallocate(1100, 50);
        // 新的空闲块 [1100, 1150) 与之前的 [1030, 1100) 不相邻，无法合并
        manager.printState(); // highWaterMark=1150, free=[Free[1030, 1100) len=70, Free[1100, 1150) len=50]

        System.out.println("\n6. Deallocating block at 1000, length 30...");
        manager.deallocate(1000, 30);
        // 新的空闲块 [1000, 1030) 与 [1030, 1100) 相邻，合并！
        // 之后，合并后的 [1000, 1100) 又与 [1100, 1150) 相邻，再次合并！
        manager.printState(); // highWaterMark=1150, free=[Free[1000, 1150) len=150]

    }
}
