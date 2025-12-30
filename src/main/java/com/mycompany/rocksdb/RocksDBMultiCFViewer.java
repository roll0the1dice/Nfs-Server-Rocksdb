package com.mycompany.rocksdb;

import org.rocksdb.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RocksDBMultiCFViewer {
    static { RocksDB.loadLibrary(); }

    public static void main(String[] args) {
        String dbPath = "./fs-SP0-0-index";

        try (final Options options = new Options()) {
            // 1. 获取所有列族的名称
            List<byte[]> cfNames = RocksDB.listColumnFamilies(options, dbPath);
            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();

            System.out.println("检测到的列族(Column Families):");
            for (byte[] cfName : cfNames) {
                System.out.println("- " + new String(cfName));
                cfDescriptors.add(new ColumnFamilyDescriptor(cfName));
            }

            List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

            // 2. 打开数据库时必须传入所有列族的描述符
            try (final RocksDB db = RocksDB.openReadOnly(new DBOptions(), dbPath, cfDescriptors, cfHandles)) {

                // 3. 遍历每个列族
                for (int i = 0; i < cfHandles.size(); i++) {
                    ColumnFamilyHandle handle = cfHandles.get(i);
                    String cfName = new String(cfDescriptors.get(i).getName());

                    System.out.println("\n=== 正在读取列族: " + cfName + " ===");

                    if(!"default".equals(cfName)) {
                        continue;
                    }

                    try (RocksIterator iter = db.newIterator(handle)) {
                        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                            String key = new String(iter.key(), StandardCharsets.UTF_8);
                            String val = new String(iter.value(), StandardCharsets.UTF_8);
                            System.out.printf("[%s] Key: %s -> Val: %s%n", cfName, key, val);
                        }
                    }
                    // 记得关闭 handle，除了 default handle 可能会报错，通常放在最后统一处理
                }

                // 显式销毁 Handles (虽然 try-with-resources 关闭 DB 会释放，但显式处理更好)
                for (ColumnFamilyHandle handle : cfHandles) {
                    handle.close();
                }
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}
