package com.mycompany.rocksdb.physical;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.rocksdb.POJO.FileMetadata;
import com.mycompany.rocksdb.myrocksdb.MyRocksDB;
import com.mycompany.rocksdb.physical.PhysicalStorageEngine.PhysicalWriteResult;
import com.mycompany.rocksdb.utils.MetaKeyUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class PhysicalMetadataManager {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * 阶段二：持久化物理元数据到 RocksDB
     */
    public static void savePhysicalMetadata(String filename, String versionKey, PhysicalStorageEngine.PhysicalWriteResult writeResult, boolean isCreated) throws Exception {
        MyRocksDB db = MyRocksDB.getIntegration(MyRocksDB.INDEX_LUN);
        String fileMetaKey = MetaKeyUtils.getFileMetaKey(filename);
        
        FileMetadata metadata;
        if (!isCreated) {
            byte[] value = db.get(fileMetaKey.getBytes(StandardCharsets.UTF_8));
            metadata = (value != null) ? MAPPER.readValue(value, FileMetadata.class) : new FileMetadata();
        } else {
            metadata = new FileMetadata();
            metadata.setFileName(fileMetaKey);
            metadata.setMetaKey(versionKey);
            metadata.setLun(MyRocksDB.INDEX_LUN);
            metadata.setLen(new ArrayList<>());
            metadata.setOffset(new ArrayList<>());
        }

        // 更新物理索引
        metadata.getOffset().addAll(writeResult.offsets);
        metadata.getLen().addAll(writeResult.lens);
        metadata.setSize(metadata.getSize() + writeResult.totalWritten);

        // 存入 RocksDB
        db.put(fileMetaKey.getBytes(StandardCharsets.UTF_8), MAPPER.writeValueAsBytes(metadata));
    }

    public static long saveFileMetaDataOptimized(String filename, String versionKey, byte[] dataToWrite, boolean isCreated) {
        try {
            // --- 1. 数据面：物理落盘 ---
            // 不涉及 RocksDB 业务逻辑，只管写块
            PhysicalWriteResult writeResult = PhysicalStorageEngine.writeToPhysicalDevice(dataToWrite);

            // --- 2. 控制面：元数据存入 RocksDB ---
            // 只管更新 JSON 索引
            PhysicalMetadataManager.savePhysicalMetadata(filename, versionKey, writeResult, isCreated);

            // 返回第一个偏移量（兼容原有接口）
            return writeResult.offsets.get(0);

        } catch (Exception e) {
            throw new RuntimeException("Failed optimized write", e);
        }
    }
}
