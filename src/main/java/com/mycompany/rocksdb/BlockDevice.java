package com.mycompany.rocksdb;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.rocksdb.RocksDBException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
@Builder
public class BlockDevice {
    public static Map<String, BlockDevice> map = new ConcurrentHashMap<>();

    // 当前 BlockDevice 的逻辑名称或唯一标识符，用于在系统中唯一标识和管理该块设备。
    private String name;

    // 该块设备（BlockDevice）对应的底层设备文件或分区的路径。
    private String path;

    // 块设备（BlockDevice）所管理的逻辑空间的总字节数。
    private long size;

    //该字段的值来源于块设备头部的元数据解析，具体是在初始化时读取设备文件头部的 offset_move_end 标志字节来判断。
    //其含义是：
    //true：表示该块设备的 offset 元数据已经整理（或迁移）完成，空间分配信息已经全部归档到新的结构中。
    //false：表示还未完成整理，系统启动时需要进行一次 offset 数据的扫描和整理（如调用 scanMoveOffsetData() 方法）。
    private boolean offset_move_end;

    private BlockDevice(String name, String path, long size, boolean offsetMoveEnd) {
        long start = System.currentTimeMillis();
        this.name = name;
        this.path = path;
        this.size = size;

    }
}
