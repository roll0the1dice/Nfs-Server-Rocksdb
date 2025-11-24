package com.mycompany.rocksdb.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.rocksdb.POJO.LatestIndexMetadata;
import com.mycompany.rocksdb.myrocksdb.MyRocksDB;
import com.mycompany.rocksdb.utils.MetaKeyUtils;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.mycompany.rocksdb.myrocksdb.MyRocksDB.INDEX_LUN;

public class MetadataRocksStore {

    private static final Logger logger = LoggerFactory.getLogger(MetadataRocksStore.class);
    // 优化 1: 复用 ObjectMapper
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public List<LatestIndexMetadata> listDirectoryContents(String targetVnodeId, String bucket, String directoryPath) {
        List<LatestIndexMetadata> contents = new ArrayList<>();
        MyRocksDB dbWrapper = MyRocksDB.getRocksDB(INDEX_LUN); // 假设这是获取单例的方式

        if (dbWrapper == null) {
            logger.error("RocksDB instance for INDEX_LUN is not initialized.");
            return contents;
        }

        // 规范化 directoryPath，确保以 / 结尾 (根据你的业务需求决定是否需要)
        // String normalizedDirPath = directoryPath.endsWith("/") ? directoryPath : directoryPath + "/";

        try (RocksIterator iterator = dbWrapper.getRocksDB().newIterator()) {
            String searchPrefix = MetaKeyUtils.getLatestMetaKey(targetVnodeId, bucket, directoryPath);
            byte[] searchPrefixBytes = searchPrefix.getBytes(StandardCharsets.UTF_8);

            // 优化 2: 标准的 RocksDB 前缀扫描循环
            for (iterator.seek(searchPrefixBytes); iterator.isValid(); iterator.next()) {
                byte[] keyBytes = iterator.key();

                // 检查前缀是否匹配 (一旦不匹配，立即停止，因为 RocksDB 是有序的)
                if (!startsWith(keyBytes, searchPrefixBytes)) {
                    break;
                }

                try {
                    // 反序列化
                    LatestIndexMetadata metadata = objectMapper.readValue(iterator.value(), LatestIndexMetadata.class);

                    // 优化 3: 纯字符串方式判断父子关系，避免 Paths.get() 的 OS 差异
                    if (isDirectChild(metadata.getKey(), directoryPath)) {
                        contents.add(metadata);
                    }
                } catch (Exception e) {
                    // 单个脏数据不应中断整个列表的返回，记录日志即可
                    logger.warn("Failed to parse metadata for key: {}", new String(keyBytes), e);
                }
            }
        } catch (Exception e) {
            logger.error("Error listing directory contents for path: {}", directoryPath, e);
            // 根据业务决定是否抛出异常
        }

        return contents;
    }

    // 手动实现 startWiths 避免转 String 造成多余对象创建
    private boolean startsWith(byte[] source, byte[] match) {
        if (match.length > source.length) return false;
        for (int i = 0; i < match.length; i++) {
            if (source[i] != match[i]) return false;
        }
        return true;
    }

    // 纯字符串逻辑判断直接子级
    private boolean isDirectChild(String fullPath, String parentPath) {
        // 简单的逻辑示例，需要根据你的实际 Key 结构调整
        // 1. 确保 fullPath 以 parentPath 开头
        if (!fullPath.startsWith(parentPath)) return false;

        // 2. 去掉前缀
        String relative = fullPath.substring(parentPath.length());
        if (relative.startsWith("/")) relative = relative.substring(1);

        // 3. 如果剩余部分不包含 "/"，说明是直接子文件/目录
        return !relative.contains("/") && !relative.isEmpty();
    }
}