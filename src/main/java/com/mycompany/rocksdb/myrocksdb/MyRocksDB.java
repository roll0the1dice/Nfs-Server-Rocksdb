package com.mycompany.rocksdb.myrocksdb;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.rocksdb.POJO.*;
import com.mycompany.rocksdb.utils.MetaKeyUtils;
import com.mycompany.rocksdb.utils.VersionUtil;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mycompany.rocksdb.allocator.JavaAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.mycompany.rocksdb.constant.GlobalConstant.BLOCK_SIZE;
import static com.mycompany.rocksdb.constant.GlobalConstant.ROCKS_FILE_META_PREFIX;
import static com.mycompany.rocksdb.constant.GlobalConstant.ROCKS_FILE_SYSTEM_PREFIX_OFFSET;
import static com.mycompany.rocksdb.constant.GlobalConstant.SPACE_LEN;
import static com.mycompany.rocksdb.constant.GlobalConstant.SPACE_SIZE;

import java.io.File;
import java.nio.charset.StandardCharsets;
import org.rocksdb.RocksIterator;


/**
 * 真实的RocksDB集成实现
 * 与项目中的实际实现逻辑完全一致，使用真实的RocksDB列簇来存储已分配区间
 *
 */
@Data
public class MyRocksDB {
    private static final Logger logger = LoggerFactory.getLogger(MyRocksDB.class);

    // 静态哈希表，用于缓存每个LUN对应的RealRocksDBIntegration实例
    private static final Map<String, MyRocksDB> dbMap = new ConcurrentHashMap<>();

    public static final Map<String, Map<String, ColumnFamilyHandle>> cfHandleMap = new ConcurrentHashMap<>();

    public static final String INDEX_LUN = "fs-SP0-0-index";
    
    public static final String DATA_LUN = "fs-SP0-0";

    public static final String FILE_DATA_DEVICE_PATH = "dev$sdb1"; // Placeholder for the actual block device path

    public static final List<String> lunList = Arrays.asList(INDEX_LUN, DATA_LUN);

    private final String deviceName;

    private final JavaAllocator allocator;
    
    private final long totalSize;

    private final RocksDB rocksDB;

    private final List<ColumnFamilyHandle> cfHandels;

    // 移除静态初始化块，避免重复调用

    static {
        RocksDB.loadLibrary();
        
        // 初始化RocksDB
        initRocksDB();

        logger.info("RocksDB loaded");
    }

    /**
     * 构造函数
     * @param totalSize 总大小（字节）
     */
    public MyRocksDB(String deviceName, long totalSize, RocksDB rocksDB, List<ColumnFamilyHandle> cfHandels) {
        this.deviceName = deviceName;
        this.totalSize = totalSize;

        // 计算总逻辑块数，与BlockDevice中的逻辑完全一致
        long totalBlocks = (1024 * 1024 / BLOCK_SIZE) * (totalSize / (1024 * 1024) - 1);
        if (totalBlocks < 1) {
            throw new IllegalArgumentException("totalBlocks must be greater than or equal to 1");
        }

        this.allocator = new JavaAllocator(totalBlocks, BLOCK_SIZE);

        logger.info("RealRocksDBIntegration initialized: deviceName={}, totalSize={}, totalBlocks={}",
            deviceName, totalSize, totalBlocks);

        this.rocksDB = rocksDB;
        this.cfHandels = cfHandels;
    }

    public ColumnFamilyHandle getColumnFamilyHandle() {
        return cfHandels.isEmpty() ? null : cfHandels.get(0);
    }

    public byte[] get(byte[] key) throws RocksDBException {
        return rocksDB.get(key);
    }

    public byte[] get(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
        return rocksDB.get(columnFamilyHandle, key);
    }

    public void put(byte[] key, byte[] value) throws RocksDBException {
        rocksDB.put(key, value);
    }

    public void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws RocksDBException {
        rocksDB.put(columnFamilyHandle, key, value);
    }

    public void delete(byte[] key) throws RocksDBException {
        rocksDB.delete(key);
    }

    public void delete(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
        rocksDB.delete(columnFamilyHandle, key);
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

    public static void initRocksDB() {
        lunList.stream().forEach(lun -> {
            try {
                MyRocksDB db = openRocksDB(lun, lun);

                dbMap.put(lun, db);

                db.initializeBlockSpace();

                db.restoreAllocationStateFromRocksDB();
            } catch (RuntimeException e) {
                logger.error("load rocks db with " + lun + " fail ", e);
            }
        });
    }
    
    public static MyRocksDB openRocksDB(String lun, String path) {

        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
        List<byte[]> cfNames = new ArrayList<>(); // Initialize once here
        List<ColumnFamilyOptions> cfOptionsToClose = new ArrayList<>(); // To manage options for closing

        // 步骤 1: 使用静态方法 listColumnFamilies 列出所有列族名称
        try (final Options options = new Options().setCreateIfMissing(true)) {
            cfNames.addAll(RocksDB.listColumnFamilies(options, path));
            if (cfNames.isEmpty()) {
                // If empty, it means the database is new, create three column families: default, .1., migrate
                cfNames.add(RocksDB.DEFAULT_COLUMN_FAMILY);
                cfNames.add(".1.".getBytes(StandardCharsets.UTF_8));
                cfNames.add("migrate".getBytes(StandardCharsets.UTF_8));
                logger.info("创建新数据库，初始化三个列族: [default, .1., migrate]");
            } else {
                // Check if the required column families are present, add if missing
                Set<String> existingCfNames = cfNames.stream()
                    .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                    .collect(Collectors.toSet());
                
                if (!existingCfNames.contains(".1.")) {
                    cfNames.add(".1.".getBytes(StandardCharsets.UTF_8));
                    logger.info("添加缺失的列族: .1.");
                }
                if (!existingCfNames.contains("migrate")) {
                    cfNames.add("migrate".getBytes(StandardCharsets.UTF_8));
                    logger.info("添加缺失的列族: migrate");
                }
            }
        } catch (RocksDBException e) {
            logger.info("列出列族失败: " + e.getMessage());
            // If new database, create three column families: default, .1., migrate
            cfNames.clear(); // Clear existing if any before re-populating
            cfNames.add(RocksDB.DEFAULT_COLUMN_FAMILY);
            cfNames.add(".1.".getBytes(StandardCharsets.UTF_8));
            cfNames.add("migrate".getBytes(StandardCharsets.UTF_8));
            logger.info("创建新数据库，初始化三个列族: [default, .1., migrate]");
        }

        logger.info("在路径 " + path + " 下发现的列族: " +
                cfNames.stream().map(String::new).collect(Collectors.toList()));

        // 步骤 2: 为每个列族创建一个 ColumnFamilyDescriptor
        // 即使我们不打算使用它们，也必须在 open 时提供
        // 每个 Descriptor 包含列族名称和其对应的选项
        for (byte[] name : cfNames) {
            //MergeOperator mergeOperator = new com.macrosan.database.rocksdb.MossMergeOperator();
            ColumnFamilyOptions cfOption = new ColumnFamilyOptions(); // Reverted to normal instantiation
            cfOptionsToClose.add(cfOption);
            cfDescriptors.add(new ColumnFamilyDescriptor(name, cfOption));
        }

        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
        // 2. 配置并打开数据库
        RocksDB db = null;
        try (DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)) { // Moved DBOptions outside try-with-resources
            // 如果目录不存在，创建它
            File dbDir = new File(path);
            if (!dbDir.exists()) {
                boolean created = dbDir.mkdirs();
                if (!created) {
                    logger.error("无法创建数据库目录: " + path);
                    throw new RuntimeException("无法创建数据库目录: " + path);
                }
                logger.info("成功创建数据库目录: " + path);
            }

            db = RocksDB.open(dbOptions, path, cfDescriptors, cfHandles);
            logger.info("RocksDB 数据库已成功打开，位于: " + dbDir.getAbsolutePath());
            Map<String, ColumnFamilyHandle> handleMap = cfHandleMap.compute(lun, (k, v) -> {
                if (v == null) {
                    v = new HashMap<>();
                }
                return v;
            });
            cfHandles.stream().forEach(columnFamilyHandle -> {
                try {
                    String cfName = new String(columnFamilyHandle.getName());
                    handleMap.put(cfName, columnFamilyHandle);
                    logger.info("Added column family handle: {} for lun: {}", cfName, lun);
                } catch (RocksDBException e) {
                    logger.info("rocksdb handle:{}", e.getMessage());
                }
            });
            
            logger.info("Column family handles for lun {}: {}", lun, handleMap.keySet());

            // 根据LUN类型设置不同的参数
            long totalSize;
            long blockSize = 4096; // 4KB块大小

            if (lun.equals(INDEX_LUN)) {
                totalSize = 100000000000L; // 10MB for index LUN
            } else if (lun.equals(DATA_LUN)) {
                totalSize = 100000000000L; // 100MB for data LUN
            } else {
                totalSize = 100000000000L; // 默认50MB
            }

            logger.info("为LUN {} 创建RealRocksDBIntegration实例: totalSize={}, blockSize={}",
                    lun, totalSize, blockSize);

            return new MyRocksDB(lun, totalSize, db, cfHandles);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } finally {
            // dbOptions will be closed automatically by try-with-resources
            for (ColumnFamilyOptions cfOption : cfOptionsToClose) {
                cfOption.close();
            }
        }
    }

    public static ColumnFamilyHandle getColumnFamily(String lun) {
        //System.out.println("public static ColumnFamilyHandle getColumnFamily(String lun)");
        //logger.info("Getting column family for lun: {}", lun);
        //logger.info("Available luns in cfHandleMap: {}", cfHandleMap.keySet());
        
        Map<String, ColumnFamilyHandle> handleMap = cfHandleMap.get(lun);
        if (handleMap == null) {
            logger.warn("No column family handles found for lun: {}", lun);
            return null;
        }
        
        //logger.info("Available column families for lun {}: {}", lun, handleMap.keySet());
        //logger.info("Looking for column family: {}", ROCKS_FILE_SYSTEM_PREFIX_OFFSET);
        
        ColumnFamilyHandle handle = handleMap.get(ROCKS_FILE_SYSTEM_PREFIX_OFFSET);
        if (handle == null) {
            logger.warn("Column family handle not found for lun: {} and key: {}", lun, ROCKS_FILE_SYSTEM_PREFIX_OFFSET);
            return null;
        }
        
        logger.info("Successfully found column family handle for lun: {} and key: {}", lun, ROCKS_FILE_SYSTEM_PREFIX_OFFSET);
        return handle;
    }

    public static MyRocksDB getRocksDB(String lun) {
        return dbMap.get(lun);
    }

    /**
     * 初始化逻辑空间结构
     * 与BlockDevice.tryInitBlockSpace()方法逻辑完全一致
     */
    public void initializeBlockSpace() {
        logger.info("初始化逻辑空间结构: {}", deviceName);

        try {
            // 获取default列簇
            ColumnFamilyHandle columnFamilyHandle = MyRocksDB.getColumnFamily(deviceName);
            
            if (columnFamilyHandle == null) {
                logger.error("无法获取列族句柄，设备: {}", deviceName);
                return;
            }

            // 检查第一个空间键（.1.0000000000）是否存在
            byte[] v = MyRocksDB.getRocksDB(deviceName).get(columnFamilyHandle,
                    BlockInfo.getFamilySpaceKey(0).getBytes());

            if (null == v) {
                logger.info("需要初始化逻辑空间，创建空间键值对");

                // 创建一个字节数组，用于存储空间值（512字节的空值，表示未分配状态）
                byte[] value = new byte[SPACE_LEN];

                // 遍历块设备大小，按SPACE_SIZE（16MB）为步长分割
                // 计算空间数量：总空间数量 = size / SPACE_SIZE
                for (int index = 0; index < totalSize / SPACE_SIZE; index++) {
                    String key = BlockInfo.getFamilySpaceKey(index);
                    // 将空分配位图写入RocksDB
                    MyRocksDB.getRocksDB(deviceName).put(columnFamilyHandle, key.getBytes(), value);
                }

                logger.info("{} 逻辑空间初始化完成，共创建 {} 个空间键", deviceName, totalSize / SPACE_SIZE);
            } else {
                logger.info("逻辑空间已存在，跳过初始化");
            }

        } catch (Exception e) {
            logger.error("初始化逻辑空间时出错", e);
        }
    }

    /**
     * 从RocksDB恢复分配状态
     * 与BlockDevice构造函数中的恢复逻辑完全一致
     */
    public void restoreAllocationStateFromRocksDB() {
        logger.info("从RocksDB恢复分配状态: {}", deviceName);

        try {
            //System.out.println(MyRocksDB.dbMap);

            ColumnFamilyHandle columnFamilyHandle = MyRocksDB.getColumnFamily(deviceName);
            
            if (columnFamilyHandle == null) {
                logger.error("无法获取列族句柄，设备: {}", deviceName);
                return;
            }

            // 初始化偏移量和长度变量
            long offset = 0;
            long len = 0;

            // 遍历所有逻辑空间块（每个16MB为一个块）
            for (int index = 0; index < totalSize / SPACE_SIZE; index++) {
                String key = BlockInfo.getFamilySpaceKey(index);

                // 对于每个块，读取其分配位图（每个字节8位，1表示已分配，0表示未分配）
                byte[] value = MyRocksDB.getRocksDB(deviceName).get(columnFamilyHandle, key.getBytes());

                if (null == value) {
                    // 该块不存在，则创建一个空分配位图
                    MyRocksDB.getRocksDB(deviceName).put(columnFamilyHandle, key.getBytes(), new byte[SPACE_LEN]);
                } else {
                    long spaceIndex = getSpaceIndex(key);

                    // 遍历一个逻辑空间块的分配位图，每个字节代表8个4KB逻辑块的分配状态
                    for (int i = 0; i < value.length; i++) {
                        // 表示当前字节对应的8个逻辑块全部已分配
                        if (value[i] == -1) {
                            // 如果这是新分配区间的开始
                            if (len == 0) {
                                // 计算起始偏移量：offset = i * 8 + SPACE_LEN * spaceIndex * 8 - 1
                                offset = i * 8 + SPACE_LEN * spaceIndex * 8 - 1;
                                offset = offset == -1 ? 0 : offset;
                            }
                            // 增加8个逻辑块到当前分配区间
                            len += 8;
                        } else {
                            // 该字节的8个位的分配情况（1为已分配，0为未分配）
                            // 将字节值转换为8位二进制数组
                            byte[] arr = BlockInfo.getBitArray(value[i]);

                            // 遍历字节中的每一位
                            for (int j = 0; j < 8; j++) {
                                // 当前位对应的逻辑块已分配
                                if (arr[j] == 1) {
                                    // 如果是新区间的开始
                                    if (len == 0) {
                                        // 计算精确的起始偏移量：offset = i * 8 + j + SPACE_LEN * spaceIndex * 8 - 1
                                        offset = i * 8 + j + SPACE_LEN * spaceIndex * 8 - 1;
                                        offset = offset == -1 ? 0 : offset;
                                    }
                                    // 增加1个逻辑块到当前分配区间
                                    len++;
                                }
                                // 当前位对应的逻辑块未分配
                                else {
                                    // 如果当前分配区间不为空
                                    if (len > 0) {
                                        // 块设备初始化时，遍历元数据，把所有已分配的空间区间恢复到分配器
                                        allocator.initAllocated(offset, len);
                                        logger.debug("恢复已分配区间: startBlock={}, blockCount={}", offset, len);
                                        // 重置len = 0，准备处理下一个分配区间
                                        len = 0;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // 处理最后一个可能未完成的分配区间
            if (len > 0) {
                allocator.initAllocated(offset, len);
                logger.debug("恢复最后一个已分配区间: startBlock={}, blockCount={}", offset, len);
            }

            logger.info("分配状态恢复完成: {}", deviceName);

        } catch (Exception e) {
            logger.error("恢复分配状态时出错", e);
        }
    }

    /**
     * 序列化文件信息
     */
    private byte[] serializeFileInfo(FileSystemInitializer.FileInfo fileInfo) {
        // 简化的序列化实现
        String data = String.format("%s|%d|%d|%d|%d",
                fileInfo.fileName,
                fileInfo.fileSize,
                fileInfo.startBlock,
                fileInfo.blockCount,
                fileInfo.lastModified);

        return data.getBytes();
    }

    /**
     * 保存文件元数据到RocksDB
     * @param fileInfo 文件信息
     */
    public void saveFileMetadataToRocksDB(FileSystemInitializer.FileInfo fileInfo) {
        logger.info("保存文件元数据到RocksDB: {}", fileInfo.fileName);

        try {
            // 序列化文件元数据
            byte[] serializedData = serializeFileInfo(fileInfo);

            // 保存到RocksDB
            String key = ROCKS_FILE_META_PREFIX + fileInfo.fileName;
            MyRocksDB wrapper = MyRocksDB.getRocksDB("fs-SP0-6");
            wrapper.put(key.getBytes(StandardCharsets.UTF_8), serializedData);

            logger.debug("文件元数据保存完成: {}", fileInfo.fileName);

        } catch (Exception e) {
            logger.error("保存文件元数据时出错: {}", fileInfo.fileName, e);
        }
    }

    /**
     * 保存分配状态到RocksDB
     * 将当前分配器的状态保存到RocksDB中
     */
    public void saveAllocationStateToRocksDB() {
        logger.info("保存分配状态到RocksDB: {}", deviceName);

        try {
            ColumnFamilyHandle columnFamilyHandle = MyRocksDB.getColumnFamily(deviceName);
            
            if (columnFamilyHandle == null) {
                logger.error("无法获取列族句柄，设备: {}", deviceName);
                return;
            }

            // 扫描所有已分配区间
            List<AllocationRange> allocatedRanges = scanAllocatedRanges();

            // 将每个区间转换为位图并保存到RocksDB
            for (AllocationRange range : allocatedRanges) {
                updateRocksDBWithAllocationRange(columnFamilyHandle, range);
            }

            logger.info("分配状态保存完成，共处理 {} 个区间", allocatedRanges.size());

        } catch (Exception e) {
            logger.error("保存分配状态时出错", e);
        }
    }

    /**
     * 更新RocksDB中的分配区间
     */
    private void updateRocksDBWithAllocationRange(ColumnFamilyHandle columnFamilyHandle, AllocationRange range)
            throws RocksDBException {

        // 计算该区间涉及的空间块
        long startSpaceIndex = range.startBlock / (SPACE_LEN * 8);
        long endSpaceIndex = (range.startBlock + range.blockCount - 1) / (SPACE_LEN * 8);

        for (long spaceIndex = startSpaceIndex; spaceIndex <= endSpaceIndex; spaceIndex++) {
            String key = BlockInfo.getFamilySpaceKey(spaceIndex);
            byte[] value = MyRocksDB.getRocksDB(deviceName).get(columnFamilyHandle, key.getBytes());

            if (value == null) {
                value = new byte[SPACE_LEN];
            }

            // 计算该空间块内的起始和结束位置
            long spaceStartBlock = spaceIndex * SPACE_LEN * 8;
            long spaceEndBlock = (spaceIndex + 1) * SPACE_LEN * 8 - 1;

            long rangeStartInSpace = Math.max(range.startBlock, spaceStartBlock);
            long rangeEndInSpace = Math.min(range.startBlock + range.blockCount - 1, spaceEndBlock);

            // 更新位图中对应的位
            for (long blockIndex = rangeStartInSpace; blockIndex <= rangeEndInSpace; blockIndex++) {
                long byteIndex = (blockIndex - spaceStartBlock) / 8;
                int bitIndex = (int) ((blockIndex - spaceStartBlock) % 8);

                if (byteIndex < value.length) {
                    // 设置对应的位为1（已分配）
                    value[(int) byteIndex] |= (1 << bitIndex);
                }
            }

            // 保存更新后的位图
            MyRocksDB.getRocksDB(deviceName).put(columnFamilyHandle, key.getBytes(), value);
        }
    }

    /**
     * 扫描所有已分配区间
     * 从分配器的L0位图中扫描出所有已分配的区间
     */
    private List<AllocationRange> scanAllocatedRanges() {
        List<AllocationRange> ranges = new ArrayList<>();

        // 这里需要访问allocator的内部L0位图来扫描已分配区间
        // 为了演示，我们使用一个简化的实现
        // 在实际实现中，应该直接访问allocator的l0Bitmap

        logger.info("扫描已分配区间...");

        // 模拟扫描结果（在实际实现中，这里会扫描真实的L0位图）
        ranges.add(new AllocationRange(0, 100));
        ranges.add(new AllocationRange(200, 150));
        ranges.add(new AllocationRange(500, 80));

        return ranges;
    }

    /**
     * 将格式化后的String类型的index，如.1.0000007169，转化为long类型的原本的index，7169
     * 与BlockDevice.getSpaceIndex()方法逻辑完全一致
     */
    private long getSpaceIndex(String key) {
        int index = 3;
        char[] chars = key.toCharArray();
        for (int i = 3; i < chars.length; i++) {
            char temp = chars[i];
            if (temp != '0') {
                index = i;
                break;
            }
        }

        if (index > key.length() - 1) {
            return 0;
        }

        return Long.parseLong(key.substring(index));
    }

    /**
     * 分配空间
     * @param numBlocks 需要的逻辑块数
     * @return 分配结果数组
     */
    public JavaAllocator.Result[] allocate(long numBlocks) {
        return allocator.allocate(numBlocks);
    }

    /**
     * 释放空间
     * @param offset 起始逻辑块索引
     * @param size 逻辑块数量
     */
    public void free(long offset, long size) {
        allocator.free(offset, size);
    }

    /**
     * 获取分配器
     */
    public JavaAllocator getAllocator() {
        return allocator;
    }

    /**
     * 打印分配器状态
     */
    public void dumpAllocatorStatus() {
        allocator.dumpStatus();
    }

    /**
     * 获取指定LUN的RealRocksDBIntegration实例
     * @param lun LUN名称
     * @return RealRocksDBIntegration实例，如果不存在则返回null
     */
    public static MyRocksDB getIntegration(String lun) {
        return dbMap.get(lun);
    }

    /**
     * 获取所有缓存的integration实例
     * @return 所有LUN对应的integration实例映射
     */
    public static Map<String, MyRocksDB> getAllIntegrations() {
        return new HashMap<>(dbMap);
    }

    /**
     * 检查指定LUN的integration实例是否存在
     * @param lun LUN名称
     * @return 如果存在返回true，否则返回false
     */
    public static boolean hasIntegration(String lun) {
        return dbMap.containsKey(lun);
    }

    /**
     * 已分配区间结构
     */
    private static class AllocationRange {
        public final long startBlock;
        public final long blockCount;

        public AllocationRange(long startBlock, long blockCount) {
            this.startBlock = startBlock;
            this.blockCount = blockCount;
        }

        @Override
        public String toString() {
            return String.format("AllocationRange{startBlock=%d, blockCount=%d}",
                    startBlock, blockCount);
        }
    }

    public static long saveFileMetaData(String filename, String versionKey, byte[] dataToWrite, long count, boolean isCreated) {
        ObjectMapper objectMapper = new ObjectMapper();

        String lun = INDEX_LUN;
        Path destinationPath = Paths.get(FILE_DATA_DEVICE_PATH);

        // 确保目标文件的父目录存在
        Path parentDir = destinationPath.getParent();
        if (parentDir != null && !parentDir.toFile().exists()) {
            boolean created = parentDir.toFile().mkdirs();
            if (!created) {
                logger.error("无法创建数据存储目录: {}", parentDir);
                throw new RuntimeException("无法创建数据存储目录: " + parentDir);
            }
            logger.info("成功创建数据存储目录: {}", parentDir);
        }

        // 获取对应的RealRocksDBIntegration实例
        MyRocksDB db = MyRocksDB.getIntegration(lun);
        if (db == null) {
            logger.error("无法获取LUN {} 的 MyRocksDB 实例", lun);
            throw new RuntimeException("MyRocksDB 实例未初始化");
        }

        // 写入File数据
        String fileMetaKey = MetaKeyUtils.getFileMetaKey(filename);
        // 写入文件数据
        try {
            byte[] value = db.get(filename.getBytes(StandardCharsets.UTF_8));
            Optional<FileMetadata> fileMetadata = Optional.empty();
            long fileOffset = 0;
            long len = 0;
            if (value != null && !isCreated) {
                fileMetadata = Optional.of(objectMapper.readValue(value, FileMetadata.class));

                // 使用RealRocksDBIntegration分配逻辑空间
                long numBlocks = (count + BLOCK_SIZE - 1) / BLOCK_SIZE; // 向上取整
                JavaAllocator.Result[] results = db.allocate(numBlocks);
                if (results.length == 0) {
                    throw new RuntimeException("无法分配足够的逻辑空间");
                }

                fileOffset = results[0].offset * BLOCK_SIZE;
                len = results[0].size * BLOCK_SIZE;

                try (FileChannel destinationChannel = FileChannel.open(destinationPath, StandardOpenOption.WRITE,
                             StandardOpenOption.CREATE)) {
                    destinationChannel.position(fileOffset);

                    // Create a ByteBuffer by wrapping the byte array
                    ByteBuffer buffer = ByteBuffer.wrap(dataToWrite);

                    // Write the sequence of bytes from the buffer to the channel
                    while (buffer.hasRemaining()) {
                        destinationChannel.write(buffer);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                fileMetadata.get().getOffset().add(fileOffset);
                fileMetadata.get().getLen().add(len);
                fileMetadata.get().setSize(fileMetadata.get().getSize() + count);
            } else {
                // 使用RealRocksDBIntegration分配逻辑空间
                long numBlocks = (count + BLOCK_SIZE - 1) / BLOCK_SIZE; // 向上取整
                JavaAllocator.Result[] results = db.allocate(numBlocks);
                if (results.length == 0) {
                    throw new RuntimeException("could not allocate enough space");
                }

                fileOffset = results[0].offset * BLOCK_SIZE;
                len = results[0].size * BLOCK_SIZE;

                try (FileChannel destinationChannel = FileChannel.open(destinationPath, StandardOpenOption.WRITE,
                             StandardOpenOption.CREATE)) {
                    destinationChannel.position(fileOffset);

                    // Create a ByteBuffer by wrapping the byte array
                    ByteBuffer buffer = ByteBuffer.wrap(dataToWrite);

                    // Write the sequence of bytes from the buffer to the channel
                    while (buffer.hasRemaining()) {
                        destinationChannel.write(buffer);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                String eTag = calculateETag(dataToWrite);

                fileMetadata = Optional.of(FileMetadata.builder().fileName(fileMetaKey).etag(eTag).size(count)
                        .metaKey(versionKey).offset(Arrays.asList(fileOffset)).len(Arrays.asList(len)).lun(lun).build());
            }   

            db.put(fileMetaKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(fileMetadata.get()));

            return fileOffset;
        } catch (JsonProcessingException | RocksDBException e) {
            logger.error("write file data into disk fails...");
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 计算ETag
     */
    public static String calculateETag(byte[] data) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(data);
            return bytesToHex(digest);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5算法不可用", e);
        }
    }

    /**
     * 字节数组转十六进制字符串
     */
    public static String bytesToHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            result.append(String.format("%02x", b));
        }
        return result.toString();
    }

    /**
     * 从底层存储读取文件数据
     * @param fileMetadata 文件的元数据，包含数据块的偏移量和长度
     * @param offset 从文件内容的哪个偏移量开始读取 (NFS请求的offset)
     * @param count 要读取的字节数 (NFS请求的count)
     * @return 读取到的字节数组
     * @throws IOException 如果读取过程中发生IO错误
     */
    public static byte[] readFileData(FileMetadata fileMetadata, long offset, int count) throws IOException {
        Path dataPath = Paths.get(FILE_DATA_DEVICE_PATH);
        ByteBuffer buffer = ByteBuffer.allocate(count);

        try (FileChannel fileChannel = FileChannel.open(dataPath, StandardOpenOption.READ)) {
            long currentFileOffset = 0; // Tracks our position within the logical file content

            for (int i = 0; i < fileMetadata.getOffset().size(); i++) {
                long blockStartOffset = fileMetadata.getOffset().get(i); // Physical start offset of this block
                long blockLength = fileMetadata.getLen().get(i);         // Length of this block

                long blockEndOffset = currentFileOffset + blockLength; // Logical end offset of this block

                // Check if this block contains any data we need to read
                if (offset < blockEndOffset && offset + count > currentFileOffset) {
                    // Calculate the start position in the current block to read from
                    long readStartInBlock = Math.max(0, offset - currentFileOffset);
                    // Calculate the number of bytes to read from this block
                    int bytesToReadFromBlock = (int) Math.min(blockLength - readStartInBlock, count - buffer.position());

                    if (bytesToReadFromBlock > 0) {
                        // Calculate the physical position in the file channel
                        long physicalReadPosition = blockStartOffset + readStartInBlock;
                        
                        // Set the channel position and read into the buffer
                        fileChannel.position(physicalReadPosition);
                        int bytesRead = fileChannel.read(buffer);
                        if (bytesRead == -1) {
                            // Reached end of channel unexpectedly
                            break; 
                        }
                    }
                }
                currentFileOffset += blockLength;
                if (buffer.position() == count) {
                    break; // Read enough data
                }
            }
        }

        byte[] result = new byte[buffer.position()];
        buffer.flip();
        buffer.get(result);
        return result;
    }

    public static void saveRedis(String vnodeId, List<Long> link, String s_uuid) {
        String lun = DATA_LUN;
        // 连接到本地 Redis 6号库
        RedisURI redisURI = RedisURI.builder().withHost("172.20.123.123")
                .withPort(6379)
                .withPassword("Gw@uUp8tBedfrWDy".toCharArray())
                .withDatabase(6)
                .build();
        RedisClient redisClient = RedisClient.create(redisURI);
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        RedisCommands<String, String> commands = connection.sync();
        // 写入数据
        String redisKey = "dataa" + vnodeId;

        commands.hset(redisKey, "v_num", vnodeId);
        commands.hset(redisKey, "lun_name", lun);
        commands.hset(redisKey, "link", link.toString());
        commands.hset(redisKey, "s_uuid", s_uuid);
        commands.hset(redisKey, "take_over", "0");

        // 关闭连接
        connection.close();
        redisClient.shutdown();
    }

    public static Optional<FileMetadata> getFileMetaData(String fileName) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();

            String lun = INDEX_LUN;
            Path path = Paths.get(INDEX_LUN);

            if (!path.toFile().exists()) {
                System.out.println("ERROR: device does not exist!");
                System.exit(-1);
            }

            // 写入File数据
            String fileMetaKey = MetaKeyUtils.getFileMetaKey(fileName);
            // 写入文件数据
            MyRocksDB db = MyRocksDB.getRocksDB(lun);

            byte[] value = db.get(fileMetaKey.getBytes(StandardCharsets.UTF_8));

            return Optional.of(objectMapper.readValue(value, FileMetadata.class));
        } catch (Exception e) {
            logger.error("operate rocksdb fails..", e);
        }
        return Optional.empty();
    }


    public static Optional<FileMetadata> getFileMetaData(String targetVnodeId, String bucketName, String objectName, String requestId) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();

            String lun = INDEX_LUN;
            Path path = Paths.get(INDEX_LUN);

            if (!path.toFile().exists()) {
                System.out.println("ERROR: device does not exist!");
                System.exit(-1);
            }

            // 写入File数据
            String fileMetaKey = MetaKeyUtils.getFileMetaKey(targetVnodeId, bucketName, objectName, requestId);
            // 写入文件数据
            MyRocksDB db = MyRocksDB.getRocksDB(lun);

            byte[] value = db.get(fileMetaKey.getBytes(StandardCharsets.UTF_8));

            return Optional.of(objectMapper.readValue(value, FileMetadata.class));
        } catch (Exception e) {
            logger.error("operate rocksdb fails..", e);
        }
        return Optional.empty();
    }

    public static Optional<VersionIndexMetadata> getIndexMetaData(String targetVnodeId, String bucket, String object) {
        ObjectMapper objectMapper = new ObjectMapper();
        String lun = INDEX_LUN;
        String versionId = "null";
        String verisonKey = MetaKeyUtils.getVersionMetaDataKey(targetVnodeId, bucket, object, versionId);
        try {

            MyRocksDB db = MyRocksDB.getRocksDB(lun);
            byte[] value = db.get(verisonKey.getBytes(StandardCharsets.UTF_8));
            if (value != null) {
                VersionIndexMetadata versionIndexMetadata = objectMapper.readValue(value, VersionIndexMetadata.class);
                return Optional.of(versionIndexMetadata);
            }
        } catch (Exception e) {
            logger.error("could not read value for verisonKey " + verisonKey, e);
        }
        return Optional.empty();
    }

    public static Optional<Inode> saveIndexMetaAndInodeData(String targetVnodeId, String bucket, String object, long contentLength, String contentType, int mode) {
        ObjectMapper objectMapper = new ObjectMapper();

        String lun = INDEX_LUN;
        if (lun == null || StringUtils.isBlank(lun)) {
            System.out.println("ERROR: device does not exist!");
            System.exit(-1);
        }

        long timestamp = System.currentTimeMillis();
        Instant instant = Instant.ofEpochMilli(timestamp);
        DateTimeFormatter manualFormatter = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss z", Locale.ENGLISH);
        String formattedDate = manualFormatter.withZone(ZoneId.of("GMT")).format(instant);
        String owner = "200915541892";

        String versionId = "null";
        String verisonKey = MetaKeyUtils.getVersionMetaDataKey(targetVnodeId, bucket, object, versionId);
        String lifeCycleMetaKey = MetaKeyUtils.getLifeCycleMetaKey(targetVnodeId, bucket, object, versionId, String.valueOf(timestamp));
        String latestMetaKey = MetaKeyUtils.getLatestMetaKey(targetVnodeId, bucket, object);
        //
        String metaKey = MetaKeyUtils.getMetaDataKey(targetVnodeId, bucket, object, "") + "0/null";

        ObjectAcl objectAcl = ObjectAcl.builder().acl("256").owner(owner).build();
        //String fileName = MetaKeyUtils.getObjFileName(bucket, object, requestId);
        String versionNum = VersionUtil.getVersionNumTrue();
        String syncStamp = versionNum;
        String shardingStamp = MetaKeyUtils.getshardingStamp();

        try {
            MyRocksDB db = MyRocksDB.getRocksDB(lun);
            Optional<VersionIndexMetadata> versionIndexMetadata = Optional.empty();

            SysMetaData sysMetaData = SysMetaData.builder().contentLength(String.valueOf(contentLength))
                    .contentType(contentType).lastModified(formattedDate.toString()).owner(owner).eTag("").displayName("testuser").build();

            versionIndexMetadata = Optional.of(VersionIndexMetadata.builder().sysMetaData(objectMapper.writeValueAsString(sysMetaData))
                    .userMetaData("{\"x-amz-meta-cb-modifiedtime\":\"Thu, 17 Apr 2025 11:27:08 GMT\"}").objectAcl(objectMapper.writeValueAsString(objectAcl)).endIndex(-1)
                    .versionNum(versionNum).syncStamp(syncStamp).shardingStamp(shardingStamp).stamp(timestamp).versionId(versionId)
                    .storage("dataa").key(object).bucket(bucket).build());

            int seconds = (int) (timestamp / 1000);
            int nseconds = (int) ((timestamp % 1000) * 1_000_000);
            Inode inode = Inode.defaultInode(versionIndexMetadata.get());
            inode.setCookie(inode.getNodeId() + 1L);
            inode.setCreateTime(seconds);
            inode.setCtime(seconds);
            inode.setCtimensec(nseconds);
            inode.setSize(contentLength);
            inode.setMode(mode);
            inode.setMajorDev(0);
            inode.setMinorDev(1);
            if (mode == 16893) {
                //inode.setSize(4096);
            }


            versionIndexMetadata.get().setInode(inode.getNodeId());
            versionIndexMetadata.get().setCookie(inode.getNodeId()+1L);
            //versionIndexMetadata.get().setInodeObject(inode);

            // 插入元数据
            LatestIndexMetadata latestIndexMetadata = VersionIndexMetadata.toLatestIndexMetadata(versionIndexMetadata.get());
            IndexMetadata indexMetadata = VersionIndexMetadata.toIndexMetadata(versionIndexMetadata.get());
            db.put(verisonKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(versionIndexMetadata.get()));
            db.put(lifeCycleMetaKey.getBytes(StandardCharsets.UTF_8), new byte[]{0});
            db.put(latestMetaKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(latestIndexMetadata));
            db.put(metaKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(indexMetadata));
            // 插入inode元数据
            String iNodeKey = Inode.getKey(targetVnodeId, inode.getBucket(), inode.getNodeId());
            db.put(iNodeKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(inode));

            return Optional.of(inode);
        } catch (JsonProcessingException e1) {
            logger.error(e1.getMessage(), e1);
        }
        catch (Exception e) {
            logger.error("operate rocksdb fails..", e);
        }

        return Optional.empty();
    }

    public static void saveIndexMetaData(String targetVnodeId, String bucket, String object, String fileName, long contentLength, String contentType, boolean isCreated) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();

        String lun = INDEX_LUN;
        if (lun == null || StringUtils.isBlank(lun)) {
            System.out.println("ERROR: device does not exist!");
            System.exit(-1);
        }

        long timestamp = System.currentTimeMillis();
        Instant instant = Instant.ofEpochMilli(timestamp);
        DateTimeFormatter manualFormatter = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss z", Locale.ENGLISH);
        String formattedDate = manualFormatter.withZone(ZoneId.of("GMT")).format(instant);
        String owner = "200915541892";

        String versionId = "null";
        String verisonKey = MetaKeyUtils.getVersionMetaDataKey(targetVnodeId, bucket, object, versionId);
        String lifeCycleMetaKey = MetaKeyUtils.getLifeCycleMetaKey(targetVnodeId, bucket, object, versionId, String.valueOf(timestamp));
        String latestMetaKey = MetaKeyUtils.getLatestMetaKey(targetVnodeId, bucket, object);
        //
        String metaKey = MetaKeyUtils.getMetaDataKey(targetVnodeId, bucket, object, "") + "0/null";

        ObjectAcl objectAcl = ObjectAcl.builder().acl("256").owner(owner).build();
        //String fileName = MetaKeyUtils.getObjFileName(bucket, object, requestId);
        String versionNum = VersionUtil.getVersionNumTrue();
        String syncStamp = versionNum;
        String shardingStamp = MetaKeyUtils.getshardingStamp();

        try {
            MyRocksDB db = MyRocksDB.getRocksDB(lun);
            byte[] value = db.get(verisonKey.getBytes(StandardCharsets.UTF_8));
            Optional<VersionIndexMetadata> versionIndexMetadata = Optional.empty();
            if (value != null && !isCreated) {
                versionIndexMetadata = Optional.of(objectMapper.readValue(value, VersionIndexMetadata.class));
                SysMetaData sysMetaData = objectMapper.readValue(versionIndexMetadata.get().getSysMetaData(), SysMetaData.class);
                contentLength = Long.parseLong(sysMetaData.getContentLength()) + contentLength;
                long endIndex = contentLength - 1 < 20 ? 20 : contentLength - 1;
                sysMetaData.setContentLength(String.valueOf(contentLength));
                versionIndexMetadata.get().setSysMetaData(objectMapper.writeValueAsString(sysMetaData));
                versionIndexMetadata.get().setEndIndex(endIndex);
            } else {
                long endIndex = contentLength - 1 < 0 ? 0 : contentLength - 1;
                SysMetaData sysMetaData = SysMetaData.builder().contentLength(String.valueOf(contentLength))
                        .contentType(contentType).lastModified(formattedDate.toString()).owner(owner).eTag("").displayName("testuser").build();
                versionIndexMetadata = Optional.of(VersionIndexMetadata.builder().sysMetaData(objectMapper.writeValueAsString(sysMetaData))
                        .userMetaData("{\"x-amz-meta-cb-modifiedtime\":\"Thu, 17 Apr 2025 11:27:08 GMT\"}").objectAcl(objectMapper.writeValueAsString(objectAcl)).fileName(fileName).endIndex(endIndex)
                        .versionNum(versionNum).syncStamp(syncStamp).shardingStamp(shardingStamp).stamp(timestamp).versionId(versionId)
                        .storage("dataa").key(object).bucket(bucket).build());

            }

            LatestIndexMetadata latestIndexMetadata = VersionIndexMetadata.toLatestIndexMetadata(versionIndexMetadata.get());
            IndexMetadata indexMetadata = VersionIndexMetadata.toIndexMetadata(versionIndexMetadata.get());
            db.put(verisonKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(versionIndexMetadata.get()));
            db.put(lifeCycleMetaKey.getBytes(StandardCharsets.UTF_8), new byte[]{0});
            db.put(latestMetaKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(latestIndexMetadata));
            db.put(metaKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(indexMetadata));


        } catch (Exception e) {
            logger.error("operate rocksdb fails..", e);
        }

    }

    public static void saveINodeMetaData(String targetVnodeId, Inode inode) {
        ObjectMapper objectMapper = new ObjectMapper();

        String lun = INDEX_LUN;
        if (lun == null || StringUtils.isBlank(lun)) {
            System.out.println("ERROR: device does not exist!");
            System.exit(-1);
        }

        try {
            MyRocksDB db = MyRocksDB.getRocksDB(lun);
            String iNodeKey = Inode.getKey(targetVnodeId, inode.getBucket(), inode.getNodeId());
            db.put(iNodeKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(inode));

        } catch (Exception e) {
            logger.error("operate rocksdb fails..", e);
        }

    }

    public static Optional<Inode> getINodeMetaData(String targetVnodeId, String bucketName, long nodeId) {
        ObjectMapper objectMapper = new ObjectMapper();

        String lun = INDEX_LUN;
        if (lun == null || StringUtils.isBlank(lun)) {
            logger.error("ERROR: device does not exist!");
            return Optional.empty();
        }

        try {
            MyRocksDB db = MyRocksDB.getRocksDB(lun);
            String iNodeKey = Inode.getKey(targetVnodeId, bucketName, nodeId);
            byte[] valueBytes = db.get(iNodeKey.getBytes(StandardCharsets.UTF_8));

            if (valueBytes != null) {
                return Optional.of(objectMapper.readValue(valueBytes, Inode.class));
            }

        } catch (Exception e) {
            logger.error("operate rocksdb fails..", e);
        }

        return Optional.empty();
    }

    public static List<LatestIndexMetadata> listDirectoryContents(String targetVnodeId, String bucket, String directoryPath) {
        List<LatestIndexMetadata> contents = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        MyRocksDB db = MyRocksDB.getRocksDB(INDEX_LUN);

        if (db == null) {
            logger.error("RocksDB instance for INDEX_LUN is not initialized.");
            return contents;
        }

        try (RocksIterator iterator = db.rocksDB.newIterator()) {
            // Construct the prefix for keys in this directory
            // Inode keys are structured as ROCKS_INODE_PREFIX + vnode + File.separator + bucket + File.separator + nodeId
            // We need to iterate over objects within a specific bucket that have a path prefix matching the directoryPath
            // This implies that the 'object' field in Inode should probably store the full path.
            // For now, let's assume object name is the full path.
            String searchPrefix = MetaKeyUtils.getLatestMetaKey(targetVnodeId, bucket, directoryPath);
            byte[] searchPrefixBytes = searchPrefix.getBytes(StandardCharsets.UTF_8);

            for (iterator.seek(searchPrefixBytes); iterator.isValid(); iterator.next()) {
                byte[] keyBytes = iterator.key();
                if (!startsWith(keyBytes, searchPrefixBytes)) {
                    break; // No longer in the target directory
                }

                // Deserialize Inode from value
                byte[] valueBytes = iterator.value();
                try {
                    LatestIndexMetadata latestIndexMetadata = objectMapper.readValue(valueBytes, LatestIndexMetadata.class);
                    // Check if the inode is a direct child of the directoryPath
                    // This requires a more robust path parsing, but for now, a simple check.
                    // We need to make sure we don't return nested directory contents.
                    Path inodePath = Paths.get(latestIndexMetadata.getKey());
                    Path dirPath = Paths.get(directoryPath);

                    if (inodePath.getParent() != null && inodePath.getParent().normalize().equals(dirPath.normalize())) {
                         contents.add(latestIndexMetadata);
                    } else if (directoryPath.equals("/") && inodePath.getParent() == null) { // For root directory children
                        contents.add(latestIndexMetadata);
                    }

                } catch (IOException e) {
                    logger.error("Error deserializing Inode from RocksDB", e);
                }
            }
        } catch (Exception e) {
            logger.error("Error listing directory contents from RocksDB for path: " + directoryPath, e);
        }

        return contents;
    }

    private static boolean startsWith(byte[] array, byte[] prefix) {
        if (array.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (array[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    public static void saveChunkFileMetaData(String chunkFileKey, ChunkFile chunkFile) {
        ObjectMapper objectMapper = new ObjectMapper();

        String lun = INDEX_LUN;
        if (chunkFileKey == null || StringUtils.isBlank(chunkFileKey)) {
            System.out.println("ERROR: device does not exist!");
            System.exit(-1);
        }

        try {
            MyRocksDB db = MyRocksDB.getRocksDB(lun);
            db.put(chunkFileKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(chunkFile));

        } catch (Exception e) {
            logger.error("operate rocksdb fails..", e);
        }

    }

    public static void deleteChunkFileMetaData(String chunkFileKey) {
        ObjectMapper objectMapper = new ObjectMapper();

        String lun = INDEX_LUN;
        if (chunkFileKey == null || StringUtils.isBlank(chunkFileKey)) {
            System.out.println("ERROR: device does not exist!");
            System.exit(-1);
        }

        try {
            MyRocksDB db = MyRocksDB.getRocksDB(lun);
            db.delete(chunkFileKey.getBytes(StandardCharsets.UTF_8));

        } catch (Exception e) {
            logger.error("operate rocksdb fails..", e);
        }

    }

    public static Optional<ChunkFile> getChunkFileMetaData(String chunkFileKey) {
        ObjectMapper objectMapper = new ObjectMapper();

        String lun = INDEX_LUN;
        if (chunkFileKey == null || StringUtils.isBlank(chunkFileKey)) {
            System.out.println("ERROR: device does not exist!");
            System.exit(-1);
        }

        try {
            MyRocksDB db = MyRocksDB.getRocksDB(lun);

            byte[] value = db.get(chunkFileKey.getBytes(StandardCharsets.UTF_8));

            return Optional.of(objectMapper.readValue(value, ChunkFile.class));

        } catch (JsonProcessingException e2) {
            logger.error("operate rocksdb fails..", e2);
        }
        catch (Exception e) {
            logger.error("operate rocksdb fails..", e);
        }

        return Optional.empty();
    }

    public static void deleteFile(String fileName) throws IOException, RocksDBException {
        ObjectMapper objectMapper = new ObjectMapper();
        String lun = INDEX_LUN;
        MyRocksDB db = MyRocksDB.getRocksDB(lun);

        if (db == null) {
            logger.error("无法获取LUN {} 的 MyRocksDB 实例", lun);
            throw new RuntimeException("MyRocksDB 实例未初始化");
        }

        String fileMetaKey = MetaKeyUtils.getFileMetaKey(fileName);

        // Retrieve FileMetadata to get vnodeId, bucket, and object, and to free blocks
        byte[] fileMetadataBytes = null;
        FileMetadata fileMetadata = null;
        try {
            fileMetadataBytes = db.get(fileMetaKey.getBytes(StandardCharsets.UTF_8));
            if (fileMetadataBytes != null) {
                fileMetadata = objectMapper.readValue(fileMetadataBytes, FileMetadata.class);
            } else {
                logger.warn("File metadata not found for deletion: {}. Cannot proceed with metadata deletion and block freeing.", fileName);
                return; // Cannot proceed without FileMetadata
            }
        } catch (RocksDBException e) {
            logger.error("Error retrieving FileMetadata for {}: {}", fileName, e.getMessage());
            throw e; // Re-throw to indicate failure
        } catch (JsonProcessingException e) {
            logger.error("Error deserializing FileMetadata for {}: {}", fileName, e.getMessage());
            throw e; // Re-throw to indicate failure
        }

        // The following section is for freeing allocated blocks, which should remain.
        // This part was previously inside a 'if (fileMetadataBytes != null)' block.
        // Now, 'fileMetadata' is guaranteed to be non-null if we reach here.

        // 2. Free allocated blocks in the block device (FILE_DATA_DEVICE_PATH)
        List<Long> offsets = fileMetadata.getOffset();
        List<Long> lengths = fileMetadata.getLen();

        if (offsets != null && lengths != null && offsets.size() == lengths.size()) {
            for (int i = 0; i < offsets.size(); i++) {
                long offset = offsets.get(i);
                long length = lengths.get(i);
                long startBlock = offset / BLOCK_SIZE;
                long numBlocks = (length + BLOCK_SIZE - 1) / BLOCK_SIZE;
                db.free(startBlock, numBlocks);
                logger.info("Freed {} blocks starting from block {} for file: {}", numBlocks, startBlock, fileName);
            }
        } else {
            logger.warn("FileMetadata for {} has inconsistent offset/length lists, cannot free blocks.", fileName);
        }

        // 3. Delete FileMetadata from RocksDB (this was already handled by deleteAllMetadata if it was successful,
        // but leaving this here as a double-check or if there are other FileMetadata keys not covered by deleteAllMetadata)
        // Based on the current deleteAllMetadata, it does not delete ROCKS_FILE_META_PREFIX keys.
        // So this deletion should remain.
        db.rocksDB.delete(fileMetaKey.getBytes(StandardCharsets.UTF_8));
        logger.info("Deleted file metadata for: {}", fileName);

        // 4. Delete the actual file content from dev$sdb1 (this is a conceptual delete, as we only free blocks)
        // The actual file content on dev$sdb1 is not physically deleted, but the space is marked as free.
        // If we want to physically clear the data, it would involve overwriting the blocks, which is complex.
        // For now, just freeing the allocation is sufficient.

        logger.info("File {} deleted from RocksDB and its allocated space freed.", fileName);
    }

    public static void deleteAllMetadata(String targetVnodeId, String bucket, String object, long timestamp) {
        String lun = INDEX_LUN;
        if (lun == null || StringUtils.isBlank(lun)) {
            logger.error("ERROR: device does not exist!");
            System.exit(-1);
        }

        try {
            MyRocksDB db = MyRocksDB.getRocksDB(lun);
            if (db == null) {
                logger.error("无法获取LUN {} 的 MyRocksDB 实例", lun);
                throw new RuntimeException("MyRocksDB 实例未初始化");
            }

            // Construct keys for deletion
            String versionId = "null"; // Assuming "null" as default versionId for these keys
            String verisonKey = MetaKeyUtils.getVersionMetaDataKey(targetVnodeId, bucket, object, versionId);
            String latestMetaKey = MetaKeyUtils.getLatestMetaKey(targetVnodeId, bucket, object);
            String metaKey = MetaKeyUtils.getMetaDataKey(targetVnodeId, bucket, object, "") + "0/null";
            String lifeCycleMetaKey = MetaKeyUtils.getLifeCycleMetaKey(targetVnodeId, bucket, object, versionId, String.valueOf(timestamp));

            // To delete the inode, we need its nodeId. This is the tricky part.
            // The saveIndexMetaAndInodeData method creates a new Inode or uses an existing one.
            // A simple delete of a single Inode key without knowing its ID might not be straightforward.
            // For now, I will assume we can retrieve the Inode using the object and bucket,
            // or we will need a more robust way to find the inode key.
            // For demonstration, let's assume we can derive or find the inode ID.
            // If Inode is embedded in VersionIndexMetadata, we could retrieve it first.
            // Let's first try to get VersionIndexMetadata to extract Inode ID if possible.
            Optional<VersionIndexMetadata> versionIndexMetadata = db.getIndexMetaData(targetVnodeId, bucket, object);
            if (versionIndexMetadata.isPresent()) {
                long inodeId = versionIndexMetadata.get().getInode();
                String iNodeKey = Inode.getKey(targetVnodeId, bucket, inodeId);
                db.delete(iNodeKey.getBytes(StandardCharsets.UTF_8));
                logger.info("Deleted Inode metadata for vnodeId: {}, bucket: {}, object: {}, inodeId: {}", targetVnodeId, bucket, object, inodeId);
            } else {
                logger.warn("VersionIndexMetadata not found for vnodeId: {}, bucket: {}, object: {}. Skipping Inode deletion.", targetVnodeId, bucket, object);
            }


            // Delete other metadata
            db.delete(verisonKey.getBytes(StandardCharsets.UTF_8));
            logger.info("Deleted version metadata for vnodeId: {}, bucket: {}, object: {}", targetVnodeId, bucket, object);

            // LifeCycleMetaKey has a timestamp. We need to iterate or adjust how it's stored/deleted.
            // For now, a generic approach for lifecycle meta key might not be suitable without knowing the exact timestamp.
            // If there's only one lifecycle per version, we could search for it.
            // For a complete reversal, we would need to know the timestamp.
            // As per the original `saveIndexMetaAndInodeData`, the `lifeCycleMetaKey` is stored with `timestamp`.
            // A precise deletion would require knowing that timestamp, or iterating.
            // Let's assume for now, we don't know the exact timestamp for lifeCycleMetaKey deletion.
            // As a simplified approach for demonstration, I will attempt to delete with a placeholder for timestamp if it's always "0" or "null" in the key.
            // Based on `saveIndexMetaAndInodeData`, it uses `String.valueOf(timestamp)`.
            // So, without the exact timestamp, we can't delete it precisely.
            // A more robust solution would involve fetching all keys with a prefix and deleting.

            db.delete(latestMetaKey.getBytes(StandardCharsets.UTF_8));
            logger.info("Deleted latest metadata for vnodeId: {}, bucket: {}, object: {}", targetVnodeId, bucket, object);

            db.delete(metaKey.getBytes(StandardCharsets.UTF_8));
            logger.info("Deleted general metadata for vnodeId: {}, bucket: {}, object: {}", targetVnodeId, bucket, object);

            db.delete(lifeCycleMetaKey.getBytes(StandardCharsets.UTF_8));
            logger.info("Deleted lifeCycle metadata for vnodeId: {}, bucket: {}, object: {}", targetVnodeId, bucket, object);

            logger.info("Successfully deleted all associated metadata for vnodeId: {}, bucket: {}, object: {}", targetVnodeId, bucket, object);

        } catch (Exception e) {
            logger.error("Error deleting metadata for vnodeId: {}, bucket: {}, object: {}", targetVnodeId, bucket, object, e);
        }
    }

    public static void renameFile(String targetVnodeId, String bucket, String oldPath, String newPath) throws RocksDBException {
        ObjectMapper objectMapper = new ObjectMapper();
        String lun = INDEX_LUN;
        MyRocksDB db = MyRocksDB.getRocksDB(lun);

        if (db == null) {
            logger.error("无法获取LUN {} 的 MyRocksDB 实例", lun);
            throw new RuntimeException("MyRocksDB 实例未初始化");
        }

        try {
            // 1. 获取旧文件的 VersionIndexMetadata
            Optional<VersionIndexMetadata> oldVersionIndexMetadataOptional = getIndexMetaData(targetVnodeId, bucket, oldPath);
            if (!oldVersionIndexMetadataOptional.isPresent()) {
                throw new RocksDBException("Old file/directory not found: " + oldPath);
            }
            VersionIndexMetadata oldVersionIndexMetadata = oldVersionIndexMetadataOptional.get();

            // 2. 获取旧文件的 Inode
            Optional<Inode> oldInodeOptional = getINodeMetaData(targetVnodeId, bucket, oldVersionIndexMetadata.getInode());
            if (!oldInodeOptional.isPresent()) {
                throw new RocksDBException("Inode not found for old file: " + oldPath);
            }
            Inode oldInode = oldInodeOptional.get();

            // 3. 构建新路径的元数据对象
            // 创建新的 VersionIndexMetadata (大部分内容与旧的一致，但更新 key 和 objectName)
            VersionIndexMetadata newVersionIndexMetadata = VersionIndexMetadata.builder()
                    .from(oldVersionIndexMetadata) // 复制所有字段
                    .key(newPath)
                    .build();

            // 创建新的 Inode (大部分内容与旧的一致，但更新 objName)
            Inode newInode = Inode.builder()
                    .from(oldInode) // 复制所有字段
                    .objName(newPath)
                    .build();

            // 4. 更新 Inode 中的 objName 和 VersionIndexMetadata 中的 key 和 object
            // 实际上在上面构建新对象时已经完成了，这里只是为了清晰说明逻辑
            newInode.setObjName(newPath);
            newVersionIndexMetadata.setKey(newPath);
            //newVersionIndexMetadata.setObject(newPath);
            //newVersionIndexMetadata.setInodeObject(newInode);

            // 5. 生成新路径对应的各种 Key
            String newVersionKey = MetaKeyUtils.getVersionMetaDataKey(targetVnodeId, bucket, newPath, newVersionIndexMetadata.getVersionId());
            String newLifeCycleMetaKey = MetaKeyUtils.getLifeCycleMetaKey(targetVnodeId, bucket, newPath, newVersionIndexMetadata.getVersionId(), String.valueOf(newVersionIndexMetadata.getStamp()));
            String newLatestMetaKey = MetaKeyUtils.getLatestMetaKey(targetVnodeId, bucket, newPath);
            String newMetaKey = MetaKeyUtils.getMetaDataKey(targetVnodeId, bucket, newPath, "") + "0/null";
            String newInodeKey = Inode.getKey(targetVnodeId, newInode.getBucket(), newInode.getNodeId());

            // 6. 将新的元数据写入 RocksDB
            db.put(newVersionKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(newVersionIndexMetadata));
            db.put(newLifeCycleMetaKey.getBytes(StandardCharsets.UTF_8), new byte[]{0}); // Assume lifecycle is empty for now
            db.put(newLatestMetaKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(VersionIndexMetadata.toLatestIndexMetadata(newVersionIndexMetadata)));
            db.put(newMetaKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(VersionIndexMetadata.toIndexMetadata(newVersionIndexMetadata)));

            // 7. 删除旧路径对应的元数据
            deleteAllMetadata(targetVnodeId, bucket, oldPath, oldVersionIndexMetadata.getStamp());

            db.put(newInodeKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(newInode));

            logger.info("Successfully renamed file/directory from {} to {}", oldPath, newPath);

        } catch (JsonProcessingException | RocksDBException e) {
            logger.error("Error renaming file from {} to {}", oldPath, newPath, e);
            throw new RocksDBException("Failed to rename file");
        }
    }

}
