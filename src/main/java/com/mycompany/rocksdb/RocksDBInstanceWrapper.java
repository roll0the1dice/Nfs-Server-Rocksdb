package com.mycompany.rocksdb;

import lombok.Data;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import java.util.List;


@Data
public class RocksDBInstanceWrapper {
    private final RocksDB rocksDB;
    private final List<ColumnFamilyHandle> cfHandels;

    public RocksDBInstanceWrapper(RocksDB rocksDB, List<ColumnFamilyHandle> cfHandels) {
        this.rocksDB = rocksDB;
        this.cfHandels = cfHandels;
    }

    public void close() {
        // 关键：先关闭所有句柄，再关闭数据库实例
        Exception closeException = null;

        // 1. 关闭所有 ColumnFamilyHandle
        for (final ColumnFamilyHandle columnFamilyHandle : cfHandels) {
            try {
                columnFamilyHandle.close();
            } catch (Exception e) {
                System.err.println("Error closing ColumnFamilyHandle: " + e.getMessage());
                if (closeException == null) {
                    closeException = new RuntimeException("Failed to close one or more resources.");
                }
                closeException.addSuppressed(e);
            }
        }

        // 2. 关闭 RocksDB 实例
        if (rocksDB != null) {
            try {
                rocksDB.close();
            } catch (Exception e) {
                System.err.println("Error closing rocksdb instance: " + e.getMessage());
                if (closeException == null) {
                    closeException = new RuntimeException("Failed to close one or more resources.");
                }
                closeException.addSuppressed(e);
            }
        }

        if (closeException != null) {
            throw (RuntimeException) closeException;
        }
    }
}
