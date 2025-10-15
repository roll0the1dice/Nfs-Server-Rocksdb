package com.mycompany.rocksdb.myrocksdb;


import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.rocksdb.POJO.*;
import com.mycompany.rocksdb.utils.MetaKeyUtils;
import com.mycompany.rocksdb.utils.VersionUtil;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.extern.log4j.Log4j2;

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


/**
 * 真实的RocksDB集成实现
 * 与项目中的实际实现逻辑完全一致，使用真实的RocksDB列簇来存储已分配区间
 *
 */
public class MyRocksDB {
    private static final Logger logger = LoggerFactory.getLogger(MyRocksDB.class);

    // 静态哈希表，用于缓存每个LUN对应的RealRocksDBIntegration实例
    private static final Map<String, MyRocksDB> dbMap = new ConcurrentHashMap<>();

    public static final Map<String, Map<String, ColumnFamilyHandle>> cfHandleMap = new ConcurrentHashMap<>();

    public static final String INDEX_LUN = "fs-SP0-14-index";
    
    public static final String DATA_LUN = "fs-SP0-9";

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
        List<byte[]> cfNames;

        // 步骤 1: 使用静态方法 listColumnFamilies 列出所有列族名称
        try {
            cfNames = RocksDB.listColumnFamilies(new Options().setCreateIfMissing(true), path);
            if (cfNames.isEmpty()) {
                // 如果为空，可能意味着数据库是新的，创建三个列族：default, .1., migrate
                cfNames = new ArrayList<>();
                cfNames.add(RocksDB.DEFAULT_COLUMN_FAMILY);
                cfNames.add(".1.".getBytes(StandardCharsets.UTF_8));
                cfNames.add("migrate".getBytes(StandardCharsets.UTF_8));
                logger.info("创建新数据库，初始化三个列族: [default, .1., migrate]");
            } else {
                // 检查是否包含所需的列族，如果缺少则添加
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
            // 如果是新数据库，创建三个列族：default, .1., migrate
            cfNames = new ArrayList<>();
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
            cfDescriptors.add(new ColumnFamilyDescriptor(name, new ColumnFamilyOptions()));
        }

        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
        // 2. 配置并打开数据库
        // 使用 try-with-resources 语句可以确保资源（如 Options 和 RocksDB 实例）被自动关闭
        //try (final Statistics statistics = new Statistics()) {
        try (final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)) {
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

            RocksDB db = RocksDB.open(dbOptions, path, cfDescriptors, cfHandles);
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
                totalSize = 1000000000; // 10MB for index LUN
            } else if (lun.equals(DATA_LUN)) {
                totalSize = 1000000000; // 100MB for data LUN
            } else {
                totalSize = 500000000; // 默认50MB
            }

            logger.info("为LUN {} 创建RealRocksDBIntegration实例: totalSize={}, blockSize={}",
                    lun, totalSize, blockSize);

            return new MyRocksDB(lun, totalSize, db, cfHandles);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        // }
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

    public static long saveFileMetaData(String fileName, String verisonKey, byte[] dataToWrite, long count, boolean isCreated) {
        ObjectMapper objectMapper = new ObjectMapper();

        String lun = DATA_LUN;
        Path destinationPath = Paths.get(DATA_LUN);

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
        String fileMetaKey = MetaKeyUtils.getFileMetaKey(fileName);
        long fileOffset = 0;
        // 写入文件数据
        try {
            byte[] value = db.get(fileMetaKey.getBytes(StandardCharsets.UTF_8));
            Optional<FileMetadata> fileMetadata = Optional.empty();
            if (value != null && !isCreated) {
                fileMetadata = Optional.of(objectMapper.readValue(value, FileMetadata.class));

                // 使用RealRocksDBIntegration分配逻辑空间
                long numBlocks = (count + BLOCK_SIZE - 1) / BLOCK_SIZE; // 向上取整
                JavaAllocator.Result[] results = db.allocate(numBlocks);
                if (results.length == 0) {
                    throw new RuntimeException("无法分配足够的逻辑空间");
                }

                long len = results[0].size * BLOCK_SIZE;
                fileOffset = results[0].offset * BLOCK_SIZE;

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
                    throw new RuntimeException("无法分配足够的逻辑空间");
                }

                long len = results[0].size * BLOCK_SIZE;
                fileOffset = results[0].offset * BLOCK_SIZE;

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

                fileMetadata = Optional.of(FileMetadata.builder().fileName(fileName).etag(eTag).size(count)
                        .metaKey(verisonKey).offset(Arrays.asList(fileOffset)).len(Arrays.asList(len)).lun(lun).build());
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

            String lun = DATA_LUN;
            Path path = Paths.get(DATA_LUN);

            if (!path.toFile().exists()) {
                System.out.println("ERROR: device does not exist!");
                System.exit(-1);
            }

            // 写入File数据
            String fileMetaKey = MetaKeyUtils.getFileMetaKey(fileName);
            long fileOffset = 0;
            // 写入文件数据
            MyRocksDB db = MyRocksDB.getRocksDB(lun);

            byte[] value = db.get(fileMetaKey.getBytes(StandardCharsets.UTF_8));

            return Optional.of(objectMapper.readValue(value, FileMetadata.class));
        } catch (Exception e) {
            logger.error("operate rocksdb fails..", e);
        }
        return Optional.empty();
    }

    public Optional<VersionIndexMetadata> getIndexMetaData(String targetVnodeId, String bucket, String object) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            String lun = INDEX_LUN;
            String versionId = "null";
            String verisonKey = MetaKeyUtils.getVersionMetaDataKey(targetVnodeId, bucket, object, versionId);
            MyRocksDB db = MyRocksDB.getRocksDB(lun);
            byte[] value = db.get(verisonKey.getBytes(StandardCharsets.UTF_8));
            if (value != null) {
                VersionIndexMetadata versionIndexMetadata = objectMapper.readValue(value, VersionIndexMetadata.class);
                return Optional.of(versionIndexMetadata);
            }
        } catch (Exception e) {
            logger.error("operate rocksdb fails..", e);
        }
        return Optional.empty();
    }

    public static Optional<Inode> saveIndexMetaAndInodeData(String targetVnodeId, String bucket, String object, String fileName, long contentLength, String contentType, boolean isCreated) throws JsonProcessingException {
        long maxOffset = 0;
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
                    .versionNum(versionNum).syncStamp(syncStamp).shardingStamp(shardingStamp).stamp(timestamp)
                    .storage("dataa").key(object).bucket(bucket).build());

            int seconds = (int) (timestamp / 1000);
            int nseconds = (int) ((timestamp % 1000) * 1_000_000);
            Inode inode = Inode.defaultInode(versionIndexMetadata.get());
            inode.setCookie(inode.getNodeId() + 1L);
            inode.setCreateTime(seconds);
            inode.setCtime(seconds);
            inode.setCtimensec(nseconds);


            versionIndexMetadata.get().setInode(inode.getNodeId());
            versionIndexMetadata.get().setCookie(inode.getNodeId()+1L);

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
        } catch (Exception e) {
            logger.error("operate rocksdb fails..", e);
        }

        return Optional.empty();
    }

    public static void saveIndexMetaData(String targetVnodeId, String bucket, String object, String fileName, long contentLength, String contentType, boolean isCreated) throws JsonProcessingException {
        long maxOffset = 0;
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
                        .versionNum(versionNum).syncStamp(syncStamp).shardingStamp(shardingStamp).stamp(timestamp)
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

    public static void saveINodeMetaData(String targetVnodeId, Inode inode) throws JsonProcessingException {
        long maxOffset = 0;
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
    public static void saveChunkFileMetaData(String chunkFileKey, ChunkFile chunkFile) throws JsonProcessingException {
        long maxOffset = 0;
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

    public static Optional<ChunkFile> getChunkFileMetaData(String chunkFileKey) throws JsonProcessingException {
        long maxOffset = 0;
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

        } catch (Exception e) {
            logger.error("operate rocksdb fails..", e);
        }

        return Optional.empty();
    }
}
