package com.mycompany.rocksdb;

// 代表一个空闲的逻辑空间区间
public class FreeInterval implements Comparable<FreeInterval> {
    private long startOffset;
    private long length;

    public FreeInterval(long startOffset, long length) {
        if (length <= 0) {
            throw new IllegalArgumentException("Length must be positive.");
        }
        this.startOffset = startOffset;
        this.length = length;
    }

    // Getters...
    public long getStartOffset() { return startOffset; }
    public long getLength() { return length; }
    public long getEndOffset() { return startOffset + length; }

    // Setters for merging
    public void setStartOffset(long startOffset) { this.startOffset = startOffset; }
    public void setLength(long length) { this.length = length; }

    // 按 startOffset 排序
    @Override
    public int compareTo(FreeInterval other) {
        return Long.compare(this.startOffset, other.startOffset);
    }

    @Override
    public String toString() {
        return String.format("Free[%d, %d) len=%d", startOffset, getEndOffset(), length);
    }
}
