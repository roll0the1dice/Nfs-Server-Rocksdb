package com.mycompany.rocksdb.myrocksdb;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.rocksdb.FreeListSpaceManager;
import com.mycompany.rocksdb.MD5Util;
import com.mycompany.rocksdb.POJO.*;
import com.mycompany.rocksdb.RocksDBInstanceWrapper;
import com.mycompany.rocksdb.utils.MetaKeyUtils;
import com.mycompany.rocksdb.utils.VersionUtil;
import io.lettuce.core.RedisURI;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import static com.mycompany.rocksdb.constant.GlobalConstant.BLOCK_SIZE;

/**
 * Hello world!
 *
 */
public class MyRocksDB
{
    private static final Logger logger = LoggerFactory.getLogger(com.mycompany.rocksdb.myrocksdb.MyRocksDB.class);

    static {
        RocksDB.loadLibrary();
    }

    private static final String DB_PATH = "/fs-SP0-2/rocks_db"; // 数据库文件存放目录

    private static final String INDEX_DB_PATH = "/fs-SP0-8-index/rocks_db";

    private static final String E_TAG = "85c45b74e8753920570f6c9a01ca759b";

    private static final Map<String, Map<String, ColumnFamilyHandle>> cfHandleMap = new ConcurrentHashMap<>();

    public static Map<String, RocksDBInstanceWrapper> dbMap = new ConcurrentHashMap<>();

    public static final long START_OFFSET = 2550000000L;

    public static final FreeListSpaceManager manager = new FreeListSpaceManager(START_OFFSET);


    public static final String INDEX_LUN = "fs-SP0-8-index";
    public static final String DATA_LUN = "fs-SP0-4";
    public static final Path SAVE_TO_DATA_PATH = Paths.get("/dev/sdg2");

    public MyRocksDB() {
        init();
    }

    public void initRocksDB() {
        List<String> lunList = Arrays.asList(INDEX_LUN, DATA_LUN);

        lunList.stream().forEach(lun -> {
            try {
                RocksDBInstanceWrapper db = openRocksDB(lun, "/" + lun + "/rocks_db");
                dbMap.put(lun, db);
            } catch (RuntimeException e) {
                logger.error("load rocks db with " + lun + " fail ", e);
            }
        });
    }

    public static RocksDBInstanceWrapper openRocksDB(String lun, String path) {

        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
        List<byte[]> cfNames = Collections.emptyList();

        // 步骤 1: 使用静态方法 listColumnFamilies 列出所有列族名称
        try {
            cfNames = RocksDB.listColumnFamilies(new Options(), path);
            if (cfNames.isEmpty()) {
                // 如果为空，可能意味着数据库是新的，至少要添加 default 列族
                cfNames.add(RocksDB.DEFAULT_COLUMN_FAMILY);
            }
        } catch (RocksDBException e) {
            logger.info("列出列族失败: " + e.getMessage());
            System.exit(-1);
        }

        logger.info("在路径 " + path + " 下发现的列族: " +
                cfNames.stream().map(String::new).collect(Collectors.toList()));

        // 步骤 2: 为每个列族创建一个 ColumnFamilyDescriptor
        // 即使我们不打算使用它们，也必须在 open 时提供
        // 每个 Descriptor 包含列族名称和其对应的选项
        for (byte[] name : cfNames) {
            MergeOperator mergeOperator = new com.macrosan.database.rocksdb.MossMergeOperator();
            cfDescriptors.add(new ColumnFamilyDescriptor(name, new ColumnFamilyOptions().setMergeOperator(mergeOperator)));
        }

        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
        // 2. 配置并打开数据库
        // 使用 try-with-resources 语句可以确保资源（如 Options 和 RocksDB 实例）被自动关闭
        //try (final Statistics statistics = new Statistics()) {
        try (final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)) {
            // 如果目录不存在，创建它
            File dbDir = new File(path);
            if (!dbDir.exists()) {
                System.exit(1);
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
                    handleMap.put(new String(columnFamilyHandle.getName()), columnFamilyHandle);
                } catch (RocksDBException e) {
                    logger.info("rocksdb handle:{}", e.getMessage());
                }
            });

            return new RocksDBInstanceWrapper(db, cfHandles);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        // }
    }

    public static void closeAllDBs() {
        System.out.println("Starting to close all RocksDB instances and their handles...");
        Exception shutdownException = null;

        for (Map.Entry<String, RocksDBInstanceWrapper> entry : dbMap.entrySet()) {
            String dbName = entry.getKey();
            RocksDBInstanceWrapper wrapper = entry.getValue();

            if (wrapper != null) {
                try {
                    System.out.println("Closing RocksDB wrapper for: " + dbName);
                    wrapper.close(); // 调用封装类的关闭方法
                    System.out.println("Successfully closed wrapper for: " + dbName);
                } catch (Exception e) {
                    System.err.println("Error closing wrapper for '" + dbName + "'.");
                    if (shutdownException == null) {
                        shutdownException = new RuntimeException("Failed to close one or more RocksDB wrappers.");
                    }
                    shutdownException.addSuppressed(e);
                }
            }
        }

        dbMap.clear();
        System.out.println("Finished closing all RocksDB wrappers.");
        if (shutdownException != null) {
            throw (RuntimeException) shutdownException;
        }
    }

    /**
     * 删除 RocksDB 中所有以指定前缀开头的键值对。
     *
     * @param dbPath 数据库文件的路径
     * @param prefix 要删除的键的前缀
     * @throws RocksDBException 如果操作 RocksDB 时发生错误
     */
    public void deleteByPrefix(final String dbPath, final String prefix) throws RocksDBException {
        final byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);

        // 使用 try-with-resources 确保资源（如数据库连接）被正确关闭
        // setCreateIfMissing(false) 确保我们操作的是一个已存在的数据库
        try {

            RocksDBInstanceWrapper rocksDBInstanceWrapper = MyRocksDB.getRocksDB(INDEX_LUN);
            RocksDB db = rocksDBInstanceWrapper.getRocksDB();

            // 1. 扫描并收集所有匹配前缀的键
            final List<byte[]> keysToDelete = new ArrayList<>();
            System.out.println("Scanning for keys with prefix: '" + prefix + "'...");

            try (final RocksIterator iterator = db.newIterator()) {
                // seek() 会定位到第一个 >= prefixBytes 的键
                for (iterator.seek(prefixBytes); iterator.isValid(); iterator.next()) {
                    byte[] currentKey = iterator.key();
                    // 检查当前键是否真的以此前缀开头
                    if (startsWith(currentKey, prefixBytes)) {
                        keysToDelete.add(currentKey);
                    } else {
                        // 因为键是排序的，一旦当前键不再以此前缀开头，
                        // 后面的所有键都不会匹配，可以提前结束循环。
                        break;
                    }
                }
            }

            // 2. 如果没有找到匹配的键，则直接返回
            if (keysToDelete.isEmpty()) {
                System.out.println("No keys found with the specified prefix. Nothing to delete.");
                return;
            }

            System.out.printf("Found %d keys to delete. Preparing batch delete...%n", keysToDelete.size());

            // 3. 使用 WriteBatch 批量删除
            try (final WriteBatch batch = new WriteBatch();
                 final WriteOptions writeOptions = new WriteOptions()) {

                for (final byte[] key : keysToDelete) {
                    batch.delete(key);
                }
                // 原子地执行所有删除操作
                db.write(writeOptions, batch);
            }

            System.out.printf("Successfully deleted %d keys.%n", keysToDelete.size());

        } catch (Exception e) {
            throw new RuntimeException(e);
        } // RocksDB 连接在此处自动关闭
    }

    /**
     * 辅助方法，检查一个字节数组是否以另一个字节数组（前缀）开头。
     */
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

    public void init() {
        initRocksDB();

        String data_lun = DATA_LUN;
        long maxOffset = 0;
        ObjectMapper objectMapper = new ObjectMapper();
        RocksDBInstanceWrapper rocksDBInstanceWrapper = MyRocksDB.getRocksDB(data_lun);
        RocksDB db = rocksDBInstanceWrapper.getRocksDB();
        try (final RocksIterator iterator = db.newIterator()) {
            // 4. 从第一个元素开始遍历
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                if (key.startsWith("#")) {
                    String value = new String(iterator.value(), StandardCharsets.UTF_8);
                    //System.out.println("Key: " + key + ", Value: " + value);.
                    FileMetadata metadata = objectMapper.readValue(value, FileMetadata.class);
                    Optional<Long> maxOptional = metadata.getOffset().stream().max(Comparator.naturalOrder());
                    if (maxOptional.isPresent()) {
                        maxOffset = Math.max(maxOffset, maxOptional.get());
                    }
                }
            }


        } catch (JsonMappingException e) {
            throw new RuntimeException(e);
        } catch (JsonParseException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (maxOffset > 0) {
            maxOffset = maxOffset / BLOCK_SIZE * BLOCK_SIZE + BLOCK_SIZE;
            manager.setHighWaterMark(maxOffset);
        }

//        try {
//            deleteByPrefix("", "(36866");
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
    }

    public static void main(String[] args) throws IOException {
        // 1. 静态加载 RocksDB JNI 库
        // 这是使用 RocksDB Java API 的第一步，也是最重要的一步
        //RocksDB.loadLibrary();
        MyRocksDB myRocksDB = new MyRocksDB();
        myRocksDB.init();

        byte[] writeData = Files.readAllBytes(Paths.get("my-file.txt"));

        try {
            String bucket = "12321";
            String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);
            String object = "gadw.txt";
            String requestId = MetaKeyUtils.getRequestId();
            String filename = MetaKeyUtils.getObjFileName(bucket, object, requestId);
            myRocksDB.saveIndexMetaData(targetVnodeId, bucket, object, filename, writeData.length, "text/plain", true);

            String vnodeId = MetaKeyUtils.getObjectVnodeId(bucket, object);
            List<Long> link = Arrays.asList(((long)Long.parseLong(vnodeId)));
            String s_uuid = "0002";
            myRocksDB.saveRedis(vnodeId, link,  s_uuid);


            String versionId = "null";
            String versionKey = MetaKeyUtils.getVersionMetaDataKey(targetVnodeId, bucket, object, versionId);
            //myRocksDB.saveFileData(filename, versionKey, writeData.length, writeData, true);

        } catch (Exception e) {
            logger.error("operate db fail..");
        }

        MyRocksDB.closeAllDBs();
    }


    public static RocksDBInstanceWrapper getRocksDB(String lun) {
        return dbMap.get(lun);
    }

//    public long saveFileData(String fileName, String verisonKey, long length, byte[] payload, boolean isCreated) {
//        ObjectMapper objectMapper = new ObjectMapper();
//
//        String lun = DATA_LUN;
//        Path path = SAVE_TO_DATA_PATH;
//
//        if (!path.toFile().exists()) {
//            System.out.println("ERROR: device does not exist!");
//            System.exit(-1);
//        }
//
//        // 写入File数据
//        String fileMetaKey = MetaKeyUtils.getFileMetaKey(fileName);
//        long fileOffset = 0;
//        // 写入文件数据
//        try {
//            RocksDBInstanceWrapper rocksDBInstanceWrapper = MyRocksDB.getRocksDB(lun);
//            RocksDB db = rocksDBInstanceWrapper.getRocksDB();
//
//            byte[] value = db.get(fileMetaKey.getBytes(StandardCharsets.UTF_8));
//            Optional<FileMetadata> fileMetadata = Optional.empty();
//            if (value != null && !isCreated) {
//                fileMetadata = Optional.of(objectMapper.readValue(value, FileMetadata.class));
////                FileMetadata fileMetadata = FileMetadata.builder().fileName(fileName).etag(E_TAG).size(0)
////                        .metaKey(verisonKey).offset(new ArrayList<>()).len(new ArrayList<>()).lun(lun).build();
//                long len = length/BLOCK_SIZE*BLOCK_SIZE+BLOCK_SIZE;
//                fileOffset = manager.allocate(len).get();
//
//                try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
//                    ByteBuffer byteBuffer = ByteBuffer.wrap(payload);
//                    channel.write(byteBuffer, fileOffset);
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//                fileMetadata.get().getOffset().add(fileOffset);
//                fileMetadata.get().getLen().add(len);
//                fileMetadata.get().setSize(fileMetadata.get().getSize() + length);
//            } else {
//                long len = length/BLOCK_SIZE*BLOCK_SIZE+BLOCK_SIZE;
//                fileOffset = manager.allocate(len).get();
//
//                try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
//                    ByteBuffer byteBuffer = ByteBuffer.wrap(payload);
//                    channel.write(byteBuffer, fileOffset);
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//
//                fileMetadata = Optional.of(FileMetadata.builder().fileName(fileName).etag(E_TAG).size(length)
//                        .metaKey(verisonKey).offset(Arrays.asList(fileOffset)).len(Arrays.asList(len)).lun(lun).build());
//            }
//
//            db.put(fileMetaKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(fileMetadata.get()));
//
//            return fileOffset;
//        } catch (JsonProcessingException | RocksDBException e) {
//            logger.error("write file data into disk fails...");
//            throw new RuntimeException(e);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }

public long saveFileMetaData(String fileName, String verisonKey, byte[] dataToWrite, long count, boolean isCreated) {
    ObjectMapper objectMapper = new ObjectMapper();

    String lun = DATA_LUN;
    Path destinationPath = SAVE_TO_DATA_PATH;

    if (!destinationPath.toFile().exists()) {
        System.out.println("ERROR: device does not exist!");
        System.exit(-1);
    }

    // 写入File数据
    String fileMetaKey = MetaKeyUtils.getFileMetaKey(fileName);
    long fileOffset = 0;
    String etag = MD5Util.getMd5(dataToWrite);
    // 写入文件数据
    try {
        RocksDBInstanceWrapper rocksDBInstanceWrapper = MyRocksDB.getRocksDB(lun);
        RocksDB db = rocksDBInstanceWrapper.getRocksDB();

        byte[] value = db.get(fileMetaKey.getBytes(StandardCharsets.UTF_8));
        Optional<FileMetadata> fileMetadata = Optional.empty();
        if (value != null && !isCreated) {
            fileMetadata = Optional.of(objectMapper.readValue(value, FileMetadata.class));
//                FileMetadata fileMetadata = FileMetadata.builder().fileName(fileName).etag(E_TAG).size(0)
//                        .metaKey(verisonKey).offset(new ArrayList<>()).len(new ArrayList<>()).lun(lun).build();

            long len = count /BLOCK_SIZE*BLOCK_SIZE+BLOCK_SIZE;
            fileOffset = manager.allocate(len).get();

            ByteBuffer buffer = ByteBuffer.wrap(dataToWrite);
            // 使用 try-with-resources 确保 channel 会被自动关闭
            try (FileChannel destinationChannel = FileChannel.open(destinationPath,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {

                System.out.println("offset: " + fileOffset);
                System.out.println("data: \"" + dataToWrite.length + "\"");
                System.out.println("bytes count: " + buffer.remaining());

                // 4. ***核心操作***: 使用带 position 参数的 write 方法
                int bytesWritten = destinationChannel.write(buffer, fileOffset);

                System.out.println("actual bytes to be written: " + bytesWritten);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            fileMetadata.get().getOffset().add(fileOffset);
            fileMetadata.get().getLen().add(len);
            fileMetadata.get().setSize(fileMetadata.get().getSize() + count);
        } else {
            long len = count /BLOCK_SIZE*BLOCK_SIZE+BLOCK_SIZE;
            fileOffset = manager.allocate(len).get();

            ByteBuffer buffer = ByteBuffer.wrap(dataToWrite);
            // 使用 try-with-resources 确保 channel 会被自动关闭
            try (FileChannel destinationChannel = FileChannel.open(destinationPath,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {

                System.out.println("offset: " + fileOffset);
                System.out.println("data: \"" + dataToWrite.length + "\"");
                System.out.println("bytes count: " + buffer.remaining());

                // 4. ***核心操作***: 使用带 position 参数的 write 方法
                int bytesWritten = destinationChannel.write(buffer, fileOffset);

                System.out.println("actual bytes to be written: " + bytesWritten);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            fileMetadata = Optional.of(FileMetadata.builder().fileName(fileName).etag(etag).size(count)
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

    public long saveFileMetaData(String fileName, String verisonKey, Path sourcePath, long startWriteOffset, long count, boolean isCreated) {
        ObjectMapper objectMapper = new ObjectMapper();

        String lun = DATA_LUN;
        Path destinationPath = SAVE_TO_DATA_PATH;

        if (!destinationPath.toFile().exists() || !sourcePath.toFile().exists()) {
            System.out.println("ERROR: device does not exist!");
            System.exit(-1);
        }

        // 写入File数据
        String fileMetaKey = MetaKeyUtils.getFileMetaKey(fileName);
        long fileOffset = 0;
        // 写入文件数据
        try {
            RocksDBInstanceWrapper rocksDBInstanceWrapper = MyRocksDB.getRocksDB(lun);
            RocksDB db = rocksDBInstanceWrapper.getRocksDB();

            byte[] value = db.get(fileMetaKey.getBytes(StandardCharsets.UTF_8));
            Optional<FileMetadata> fileMetadata = Optional.empty();
            if (value != null && !isCreated) {
                fileMetadata = Optional.of(objectMapper.readValue(value, FileMetadata.class));
//                FileMetadata fileMetadata = FileMetadata.builder().fileName(fileName).etag(E_TAG).size(0)
//                        .metaKey(verisonKey).offset(new ArrayList<>()).len(new ArrayList<>()).lun(lun).build();

                long len = count /BLOCK_SIZE*BLOCK_SIZE+BLOCK_SIZE;
                fileOffset = manager.allocate(len).get();

                try (FileChannel sourceChannel = FileChannel.open(sourcePath, StandardOpenOption.READ);
                     FileChannel destinationChannel = FileChannel.open(destinationPath, StandardOpenOption.WRITE,
                                                                                        StandardOpenOption.CREATE,
                                                                                        StandardOpenOption.TRUNCATE_EXISTING)) {
                    long byteTransferred = startWriteOffset;
                    long fileWrittenBytes = count;
                    destinationChannel.position(fileOffset);

                    while (fileWrittenBytes > 0) {
                        byteTransferred += sourceChannel.transferTo(byteTransferred, fileWrittenBytes, destinationChannel);
                        fileWrittenBytes -= (byteTransferred - startWriteOffset);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                fileMetadata.get().getOffset().add(fileOffset);
                fileMetadata.get().getLen().add(len);
                fileMetadata.get().setSize(fileMetadata.get().getSize() + count);
            } else {
                long len = count /BLOCK_SIZE*BLOCK_SIZE+BLOCK_SIZE;
                fileOffset = manager.allocate(len).get();

                try (FileChannel sourceChannel = FileChannel.open(sourcePath, StandardOpenOption.READ);
                     FileChannel destinationChannel = FileChannel.open(destinationPath, StandardOpenOption.WRITE,
                             StandardOpenOption.CREATE,
                             StandardOpenOption.TRUNCATE_EXISTING)) {
                    long byteTransferred = startWriteOffset;
                    long fileWrittenBytes= count;
                    destinationChannel.position(fileOffset);

                    while (fileWrittenBytes > 0) {
                        byteTransferred += sourceChannel.transferTo(byteTransferred, fileWrittenBytes, destinationChannel);
                        fileWrittenBytes -= (byteTransferred - startWriteOffset);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                fileMetadata = Optional.of(FileMetadata.builder().fileName(fileName).etag(E_TAG).size(count)
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

    public void saveRedis123(String vnodeId, List<Long> link, String s_uuid) {
        String lun = DATA_LUN;
        // 连接到本地 Redis 6号库
        RedisURI redisURI = RedisURI.builder().withHost("localhost")
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


    public void saveRedis124(String vnodeId, List<Long> link, String s_uuid) {
        String lun = DATA_LUN;
        // 连接到本地 Redis 6号库
        RedisURI redisURI = RedisURI.builder().withHost("localhost")
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

    public void saveRedis(String vnodeId, List<Long> link, String s_uuid) {
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

    public Optional<FileMetadata> getFileMetaData(String fileName) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();

            String lun = DATA_LUN;
            Path path = SAVE_TO_DATA_PATH;

            if (!path.toFile().exists()) {
                System.out.println("ERROR: device does not exist!");
                System.exit(-1);
            }

            // 写入File数据
            String fileMetaKey = MetaKeyUtils.getFileMetaKey(fileName);
            long fileOffset = 0;
            // 写入文件数据
            RocksDBInstanceWrapper rocksDBInstanceWrapper = MyRocksDB.getRocksDB(lun);
            RocksDB db = rocksDBInstanceWrapper.getRocksDB();

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
            RocksDBInstanceWrapper rocksDBInstanceWrapper = MyRocksDB.getRocksDB(lun);
            RocksDB db = rocksDBInstanceWrapper.getRocksDB();
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

    public Optional<Inode> saveIndexMetaAndInodeData(String targetVnodeId, String bucket, String object, String fileName, long contentLength, String contentType, boolean isCreated) throws JsonProcessingException {
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
            RocksDBInstanceWrapper rocksDBInstanceWrapper = MyRocksDB.getRocksDB(lun);
            RocksDB db = rocksDBInstanceWrapper.getRocksDB();
            //byte[] value = db.get(verisonKey.getBytes(StandardCharsets.UTF_8));
            Optional<VersionIndexMetadata> versionIndexMetadata = Optional.empty();

            SysMetaData sysMetaData = SysMetaData.builder().contentLength(String.valueOf(contentLength))
                    .contentType(contentType).lastModified(formattedDate.toString()).owner(owner).eTag(E_TAG).displayName("testuser").build();
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

    public void saveIndexMetaData(String targetVnodeId, String bucket, String object, String fileName, long contentLength, String contentType, boolean isCreated) throws JsonProcessingException {
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
            RocksDBInstanceWrapper rocksDBInstanceWrapper = MyRocksDB.getRocksDB(lun);
            RocksDB db = rocksDBInstanceWrapper.getRocksDB();
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
                        .contentType(contentType).lastModified(formattedDate.toString()).owner(owner).eTag(E_TAG).displayName("testuser").build();
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

    public void saveINodeMetaData(String targetVnodeId, Inode inode) throws JsonProcessingException {
        long maxOffset = 0;
        ObjectMapper objectMapper = new ObjectMapper();

        String lun = INDEX_LUN;
        if (lun == null || StringUtils.isBlank(lun)) {
            System.out.println("ERROR: device does not exist!");
            System.exit(-1);
        }

        try {
            RocksDBInstanceWrapper rocksDBInstanceWrapper = MyRocksDB.getRocksDB(lun);
            RocksDB db = rocksDBInstanceWrapper.getRocksDB();
            String iNodeKey = Inode.getKey(targetVnodeId, inode.getBucket(), inode.getNodeId());
            db.put(iNodeKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(inode));

        } catch (Exception e) {
            logger.error("operate rocksdb fails..", e);
        }

    }
    public void saveChunkFileMetaData(String chunkFileKey, ChunkFile chunkFile) throws JsonProcessingException {
        long maxOffset = 0;
        ObjectMapper objectMapper = new ObjectMapper();

        String lun = INDEX_LUN;
        if (chunkFileKey == null || StringUtils.isBlank(chunkFileKey)) {
            System.out.println("ERROR: device does not exist!");
            System.exit(-1);
        }

        try {
            RocksDBInstanceWrapper rocksDBInstanceWrapper = MyRocksDB.getRocksDB(lun);
            RocksDB db = rocksDBInstanceWrapper.getRocksDB();
            db.put(chunkFileKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(chunkFile));

        } catch (Exception e) {
            logger.error("operate rocksdb fails..", e);
        }

    }

    public Optional<ChunkFile> getChunkFileMetaData(String chunkFileKey) throws JsonProcessingException {
        long maxOffset = 0;
        ObjectMapper objectMapper = new ObjectMapper();

        String lun = INDEX_LUN;
        if (chunkFileKey == null || StringUtils.isBlank(chunkFileKey)) {
            System.out.println("ERROR: device does not exist!");
            System.exit(-1);
        }

        try {
            RocksDBInstanceWrapper rocksDBInstanceWrapper = MyRocksDB.getRocksDB(lun);
            RocksDB db = rocksDBInstanceWrapper.getRocksDB();

            byte[] value = db.get(chunkFileKey.getBytes(StandardCharsets.UTF_8));

            return Optional.of(objectMapper.readValue(value, ChunkFile.class));

        } catch (Exception e) {
            logger.error("operate rocksdb fails..", e);
        }

        return Optional.empty();
    }

}
