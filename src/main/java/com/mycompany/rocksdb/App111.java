package com.mycompany.rocksdb;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.rocksdb.POJO.*;
import com.mycompany.rocksdb.utils.MetaKeyUtils;
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
public class App111
{
    private static final Logger logger = LoggerFactory.getLogger(App111.class);

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

    public static final Map<String, List<Long>> fileNameToLink = new ConcurrentHashMap<>();

    public void initRocksDB() {
        List<String> lunList = Arrays.asList("fs-SP0-8-index", "fs-SP0-2");

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

    public void init() {
        initRocksDB();

        String data_lun = "fs-SP0-2";
        long maxOffset = 0;
        ObjectMapper objectMapper = new ObjectMapper();
        RocksDBInstanceWrapper rocksDBInstanceWrapper = App111.getRocksDB(data_lun);
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

    }

    public static void main(String[] args) throws IOException {
        // 1. 静态加载 RocksDB JNI 库
        // 这是使用 RocksDB Java API 的第一步，也是最重要的一步
        //RocksDB.loadLibrary();
        App111 app = new App111();
        app.init();

        String index_lun = "fs-SP0-8-index";
        String data_lun = "fs-SP0-2";
        Path saveDataPath = Paths.get("/dev/sde2");

        byte[] writeData = Files.readAllBytes(Paths.get("my-file.txt"));


        try {
            String bucket = "12321";
            String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);
            String object = "gadw.txt";
            String requestId = MetaKeyUtils.getRequestId();
            String filename = MetaKeyUtils.getObjFileName(bucket, object, requestId);
            app.saveMetaData(index_lun, targetVnodeId, bucket, object, filename, writeData.length, "text/plain");

            String vnodeId = MetaKeyUtils.getObjectVnodeId(bucket, object);
            List<Long> link = Arrays.asList(((long)Long.parseLong(vnodeId)));
            String s_uuid = "0002";
            app.saveRedis(vnodeId, data_lun, link,  s_uuid);


            String versionId = "null";
            String versionKey = MetaKeyUtils.getVersionMetaDataKey(targetVnodeId, bucket, object, versionId);
            app.saveFileData(data_lun, saveDataPath, filename, versionKey, writeData.length, writeData);

        } catch (Exception e) {
            logger.error("operate db fail..");
        }

        App111.closeAllDBs();
    }


    public static RocksDBInstanceWrapper getRocksDB(String lun) {
        return dbMap.get(lun);
    }

    public void saveFileData(String lun, Path path, String fileName, String verisonKey, long length, byte[] payload) {
        ObjectMapper objectMapper = new ObjectMapper();

        if (!path.toFile().exists()) {
            System.out.println("ERROR: device does not exist!");
            System.exit(-1);
        }

        length = length < 20 ? 20 : length;

        long fileOffset = manager.allocate(length).get();

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(payload);
            channel.write(byteBuffer, fileOffset);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // 写入File数据
        String fileMetaKey = MetaKeyUtils.getFileMetaKey(fileName);
        List<Long> offset =  Arrays.asList(fileOffset);
        List<Long> len = Arrays.asList((long)(length/BLOCK_SIZE*BLOCK_SIZE+BLOCK_SIZE));
        FileMetadata fileMetadata = FileMetadata.builder().fileName(fileName).etag(E_TAG).size((long) length)
                .metaKey(verisonKey).offset(offset).len(len).lun(lun).build();

        // 写入文件数据
        try {
            RocksDBInstanceWrapper rocksDBInstanceWrapper = App111.getRocksDB(lun);
            RocksDB db = rocksDBInstanceWrapper.getRocksDB();
            db.put(fileMetaKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(fileMetadata));
        } catch (JsonProcessingException | RocksDBException e) {
            logger.error("write file data into disk fails...");
        }
    }

    public void saveRedis(String vnodeId, String lun, List<Long> link, String s_uuid) {
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

    public void saveMetaData(String lun, String targetVnodeId, String bucket, String object, String fileName, long contentLength, String contentType) throws JsonProcessingException {
        long maxOffset = 0;
        ObjectMapper objectMapper = new ObjectMapper();

        if (lun == null || StringUtils.isBlank(lun)) {
            System.out.println("ERROR: device does not exist!");
            System.exit(-1);
        }

        contentLength = contentLength < 20 ? 20 : contentLength;

        long timestamp = System.currentTimeMillis();
        Instant instant = Instant.ofEpochMilli(timestamp);
        DateTimeFormatter manualFormatter = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss z", Locale.ENGLISH);
        String formattedDate = manualFormatter.withZone(ZoneId.of("GMT")).format(instant);
        String owner = "825301384323";

        String versionId = "null";
        String verisonKey = MetaKeyUtils.getVersionMetaDataKey(targetVnodeId, bucket, object, versionId);
        String lifeCycleMetaKey = MetaKeyUtils.getLifeCycleMetaKey(targetVnodeId, bucket, object, versionId, String.valueOf(timestamp));
        String latestMetaKey = MetaKeyUtils.getLatestMetaKey(targetVnodeId, bucket, object);
        //
        String metaKey = MetaKeyUtils.getMetaDataKey(targetVnodeId, bucket, object, "") + "0/null";

        ObjectAcl objectAcl = ObjectAcl.builder().acl("256").owner(owner).build();
        //String fileName = MetaKeyUtils.getObjFileName(bucket, object, requestId);
        String versionNum = MetaKeyUtils.getVersionNum();
        String syncStamp = versionNum;
        String shardingStamp = MetaKeyUtils.getshardingStamp();

        try {
            RocksDBInstanceWrapper rocksDBInstanceWrapper = App111.getRocksDB(lun);
            RocksDB db = rocksDBInstanceWrapper.getRocksDB();
            byte[] value = db.get(verisonKey.getBytes(StandardCharsets.UTF_8));
            Optional<VersionIndexMetadata> versionIndexMetadata = Optional.empty();
            if (value != null) {
                versionIndexMetadata = Optional.of(objectMapper.readValue(value, VersionIndexMetadata.class));
                SysMetaData sysMetaData = objectMapper.readValue(versionIndexMetadata.get().getSysMetaData(), SysMetaData.class);
                contentLength = Long.parseLong(sysMetaData.getContentLength()) + contentLength;
                long endIndex = contentLength - 1 < 20 ? 20 : contentLength - 1;
                sysMetaData.setContentLength(String.valueOf(contentLength));
                versionIndexMetadata.get().setSysMetaData(objectMapper.writeValueAsString(sysMetaData));
                versionIndexMetadata.get().setEndIndex(endIndex);
            } else {
                long endIndex = contentLength - 1 < 20 ? 20 : contentLength - 1;
                SysMetaData sysMetaData = SysMetaData.builder().contentLength(String.valueOf(contentLength))
                        .contentType(contentType).lastModified(formattedDate.toString()).owner(owner).eTag(E_TAG).displayName("testuser").build();
                versionIndexMetadata = Optional.of(VersionIndexMetadata.builder().sysMetaData(objectMapper.writeValueAsString(sysMetaData))
                        .userMetaData("{\"x-amz-meta-cb-modifiedtime\":\"Thu, 17 Apr 2025 11:27:08 GMT\"}").objectAcl(objectMapper.writeValueAsString(objectAcl)).fileName(fileName).endIndex(endIndex)
                        .versionNum(versionNum).syncStamp(syncStamp).shardingStamp(shardingStamp).stamp(timestamp)
                        .storage("dataa").key(object).bucket(bucket).build());

            }

            LatestIndexMetadata latestIndexMetadata = VersionIndexMetadata.toLatestIndexMetadata(versionIndexMetadata.get());
            IndexMetadata indexMetadata = VersionIndexMetadata.toIndexMetadata(versionIndexMetadata.get());
            db.put(verisonKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(versionIndexMetadata));
            db.put(lifeCycleMetaKey.getBytes(StandardCharsets.UTF_8), new byte[]{0});
            db.put(latestMetaKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(latestIndexMetadata));
            db.put(metaKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(indexMetadata));
        } catch (Exception e) {
            logger.error("operate rocksdb fails..", e);
        }

    }

}
