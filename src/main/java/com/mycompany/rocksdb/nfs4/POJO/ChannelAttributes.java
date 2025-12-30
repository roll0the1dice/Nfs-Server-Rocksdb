<<<<<<< HEAD
package com.mycompany.rocksdb.nfs4.POJO;

import com.mycompany.rocksdb.netserver.XdrUtils;
import io.vertx.reactivex.core.buffer.Buffer;

// 定义通道属性结构 channel_attrs4
public class ChannelAttributes {
    private int headerPadSize;
    private int maxRequestSize;
    private int maxResponseSize;
    private int maxResponseSizeCached;
    private int maxOperations;
    private int maxRequests;
    private int[] rdmaIrd; // 通常是空数组

    // 构造函数或 Builder
    public ChannelAttributes(int headerPadSize, int maxRequestSize, int maxResponseSize,
                             int maxResponseSizeCached, int maxOperations, int maxRequests) {
        this.headerPadSize = headerPadSize;
        this.maxRequestSize = maxRequestSize;
        this.maxResponseSize = maxResponseSize;
        this.maxResponseSizeCached = maxResponseSizeCached;
        this.maxOperations = maxOperations;
        this.maxRequests = maxRequests;
        this.rdmaIrd = new int[0];
    }

    public void encode(Buffer buffer) {
        XdrUtils.writeInt(buffer, headerPadSize);
        XdrUtils.writeInt(buffer, maxRequestSize);
        XdrUtils.writeInt(buffer, maxResponseSize);
        XdrUtils.writeInt(buffer, maxResponseSizeCached);
        XdrUtils.writeInt(buffer, maxOperations);
        XdrUtils.writeInt(buffer, maxRequests);

        // 写入 rdma_ird 数组长度和内容
        XdrUtils.writeInt(buffer, rdmaIrd.length);
        for (int val : rdmaIrd) {
            XdrUtils.writeInt(buffer, val);
        }
    }
=======
package com.mycompany.rocksdb.nfs4.POJO;

import com.mycompany.rocksdb.netserver.XdrUtils;
import io.vertx.reactivex.core.buffer.Buffer;

// 定义通道属性结构 channel_attrs4
public class ChannelAttributes {
    private int headerPadSize;
    private int maxRequestSize;
    private int maxResponseSize;
    private int maxResponseSizeCached;
    private int maxOperations;
    private int maxRequests;
    private int[] rdmaIrd; // 通常是空数组

    // 构造函数或 Builder
    public ChannelAttributes(int headerPadSize, int maxRequestSize, int maxResponseSize,
                             int maxResponseSizeCached, int maxOperations, int maxRequests) {
        this.headerPadSize = headerPadSize;
        this.maxRequestSize = maxRequestSize;
        this.maxResponseSize = maxResponseSize;
        this.maxResponseSizeCached = maxResponseSizeCached;
        this.maxOperations = maxOperations;
        this.maxRequests = maxRequests;
        this.rdmaIrd = new int[0];
    }

    public void encode(Buffer buffer) {
        XdrUtils.writeInt(buffer, headerPadSize);
        XdrUtils.writeInt(buffer, maxRequestSize);
        XdrUtils.writeInt(buffer, maxResponseSize);
        XdrUtils.writeInt(buffer, maxResponseSizeCached);
        XdrUtils.writeInt(buffer, maxOperations);
        XdrUtils.writeInt(buffer, maxRequests);

        // 写入 rdma_ird 数组长度和内容
        XdrUtils.writeInt(buffer, rdmaIrd.length);
        for (int val : rdmaIrd) {
            XdrUtils.writeInt(buffer, val);
        }
    }
>>>>>>> 82c35694aa92253fd9c7d0c5119e5e75ab8825be
}