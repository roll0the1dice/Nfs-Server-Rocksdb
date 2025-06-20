package com.mycompany.rocksdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.rocksdb.POJO.*;
import com.mycompany.rocksdb.utils.MetaKeyUtils;
import com.mycompany.rocksdb.utils.MsVnodeUtils;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
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
import static com.mycompany.rocksdb.constant.GlobalConstant.SPACE_LEN;

/**
 * Hello world!
 *
 */
public class App 
{
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    static {
        RocksDB.loadLibrary();
    }

    private static final String DB_PATH = "/fs-SP0-2/rocks_db"; // 数据库文件存放目录

    private static final String INDEX_DB_PATH = "/fs-SP0-8-index/rocks_db";

    private static final Map<String, Map<String, ColumnFamilyHandle>> cfHandleMap = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        // 1. 静态加载 RocksDB JNI 库
        // 这是使用 RocksDB Java API 的第一步，也是最重要的一步
        //RocksDB.loadLibrary();

        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
        final List<byte[]> cfNames;

        // 步骤 1: 使用静态方法 listColumnFamilies 列出所有列族名称
        try {
            cfNames = RocksDB.listColumnFamilies(new Options(), DB_PATH);
            if (cfNames.isEmpty()) {
                // 如果为空，可能意味着数据库是新的，至少要添加 default 列族
                cfNames.add(RocksDB.DEFAULT_COLUMN_FAMILY);
            }
        } catch (RocksDBException e) {
            logger.info("列出列族失败: " + e.getMessage());
            return;
        }

        logger.info("在路径 " + DB_PATH + " 下发现的列族: " +
                cfNames.stream().map(String::new).collect(Collectors.toList()));


        // 步骤 2: 为每个列族创建一个 ColumnFamilyDescriptor
        // 即使我们不打算使用它们，也必须在 open 时提供
        // 每个 Descriptor 包含列族名称和其对应的选项
        for (byte[] name : cfNames) {
            if ("default".equals(new String(name, StandardCharsets.UTF_8))) {
                MergeOperator mergeOperator = new com.macrosan.database.rocksdb.MossMergeOperator();
                cfDescriptors.add(new ColumnFamilyDescriptor(name, new ColumnFamilyOptions().setMergeOperator(mergeOperator)));
            } else {
                cfDescriptors.add(new ColumnFamilyDescriptor(name, new ColumnFamilyOptions()));
            }
            //cfDescriptors.add(new ColumnFamilyDescriptor(name, new ColumnFamilyOptions()));
        }

        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();


        ObjectMapper objectMapper = new ObjectMapper();
        // 2. 配置并打开数据库
        // 使用 try-with-resources 语句可以确保资源（如 Options 和 RocksDB 实例）被自动关闭
        //try (final Options options = new Options().setCreateIfMissing(true)) {
        try (final Statistics statistics = new Statistics()) {
            try (final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true).setStatistics(statistics)) {
                // 如果目录不存在，创建它
                File dbDir = new File(DB_PATH);
                if (!dbDir.exists()) {
                    System.exit(1);
                }

                try (final RocksDB db = RocksDB.open(dbOptions, DB_PATH, cfDescriptors, cfHandles)) {
                    logger.info("RocksDB 数据库已成功打开，位于: " + dbDir.getAbsolutePath());


                    long maxOffset = 0;
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
                    }

                    String lun_name = "fs-SP0-2";
                    Path path = Paths.get("/dev/sde2");

                    if (!path.toFile().exists()) {
                        System.out.println("ERROR: device does not exist!");
                        System.exit(-1);
                    }

                    System.out.println("max offset: " + maxOffset);
                    // 4KB 边界对齐
                    maxOffset = maxOffset / BLOCK_SIZE * BLOCK_SIZE + BLOCK_SIZE;

                    long fileOffset = 0;
                    byte[] writeData = "hello, world!".getBytes();
                    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
                        ByteBuffer byteBuffer = ByteBuffer.wrap(writeData);
                        channel.write(byteBuffer, maxOffset);
                        fileOffset = maxOffset;
                        int writeLen = writeData.length / BLOCK_SIZE * BLOCK_SIZE + BLOCK_SIZE;
                        maxOffset += writeLen;
                    }
                    //String key = "-4436/12321/testfile.txt";
                    //String key = "#41393_12321_3a97a1b1e83fb65dd12134f8d55d3ae55955ac71_3wHWnasGntnMQtYdw8WeTgNxkg0HQRo0";
                    //String key = "*4436/12321/newkeynull";
                    //String key = "#413930_12321_3a97a1b1e83fb65dd12134f8d55d3ae55955ac71_3wHWnasGntnMQtYdw8WeTgNxkg0HQRo0";
                    //String versionMetadataKey = "*4436/12321/testfile.txt" + new String(new byte[]{0}) + "null";
                    //String key = ".1.0000000000";
                    //byte[] value = db.get(key.getBytes(StandardCharsets.UTF_8));
                    long timestamp = System.currentTimeMillis();
                    Instant instant = Instant.ofEpochMilli(timestamp);
                    DateTimeFormatter manualFormatter = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss z", Locale.ENGLISH);
                    String formattedDate = manualFormatter.withZone(ZoneId.of("GMT")).format(instant);
                    String owner = "825301384323";
                    String eTag = "85c45b74e8753920570f6c9a01ca759b";
                    String versionId = "null";
                    String bucket = "12321";
                    String object = "testfile";
                    String vnodeId = MetaKeyUtils.getObjectVnodeId(bucket, object);
                    String verisonKey = MetaKeyUtils.getVersionMetaDataKey(vnodeId, bucket, object, versionId);
                    String lifeCycleMetaKey = MetaKeyUtils.getLifeCycleMetaKey(vnodeId, bucket, object, versionId, String.valueOf(timestamp));
                    String latestMetaKey = MetaKeyUtils.getLatestMetaKey(vnodeId, bucket, object);
                    String metaKey = MetaKeyUtils.getMetaDataKey(vnodeId, bucket, object, "");
                    SysMetaData sysMetaData = SysMetaData.builder().contentLength((long)writeData.length)
                            .contentType("text/plain").lastModified(formattedDate.toString()).owner(owner).eTag("85c45b74e8753920570f6c9a01ca759b").displayName("testuser").build();
                    ObjectAcl objectAcl = ObjectAcl.builder().acl(256).owner(owner).build();
                    String requestId = MetaKeyUtils.getRequestId();
                    String fileName = MetaKeyUtils.getObjFileName(bucket, object, requestId);
                    String versionNum = MetaKeyUtils.getVersionNum();
                    String syncStamp = versionNum;
                    String shardingStamp = MetaKeyUtils.getshardingStamp();
                    VersionIndexMetadata versionIndexMetadata = VersionIndexMetadata.builder().sysMetaData(objectMapper.writeValueAsString(sysMetaData))
                            .userMetaData("").objectAcl(objectMapper.writeValueAsString(objectAcl)).fileName(fileName).endIndex(19)
                            .versionNum(versionNum).syncStamp(syncStamp).shardingStamp(shardingStamp).stamp(timestamp)
                            .storage("dataa").key("testfile.txt").bucket("12321").build();

                    LatestIndexMetadata latestIndexMetadata = VersionIndexMetadata.toLatestIndexMetadata(versionIndexMetadata);
                    IndexMetadata indexMetadata = VersionIndexMetadata.toIndexMetadata(versionIndexMetadata);



                    // 写入File数据
                    String fileMetaKey = MetaKeyUtils.getFileMetaKey(fileName);
                    List<Long> offset =  Arrays.asList(fileOffset);
                    List<Long> len = Arrays.asList((long)(writeData.length/BLOCK_SIZE*BLOCK_SIZE+BLOCK_SIZE));
                    FileMetadata fileMetadata = FileMetadata.builder().fileName(fileName).etag(eTag).size((long) writeData.length)
                            .metaKey(verisonKey).offset(offset).len(len).lun(lun_name).build();


                    // 连接到本地 Redis 6号库
                    RedisClient redisClient = RedisClient.create("redis://localhost:6379/6");
                    StatefulRedisConnection<String, String> connection = redisClient.connect();
                    RedisCommands<String, String> commands = connection.sync();
                    // 写入数据
                    String redisKey = "dataa" +
                    commands.hset("vnode123", "link", "[0,1,2]");
                    // 关闭连接
                    connection.close();
                    redisClient.shutdown();

                    // 写入元数据
                    db.put(verisonKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(versionIndexMetadata));
                    db.put(lifeCycleMetaKey.getBytes(StandardCharsets.UTF_8), new byte[SPACE_LEN]);
                    db.put(latestMetaKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(latestIndexMetadata));
                    db.put(metaKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(indexMetadata));
                    // 写入文件数据
                    db.put(fileMetaKey.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(fileMetadata));

                 //   if (value != null) {
                     //   FileMetadata fileMetadata = objectMapper.readValue(value, FileMetadata.class);
                    //    fileMetadata.getOffset().set(0, fileOffset);
                    //    db.put(key.getBytes(StandardCharsets.UTF_8), objectMapper.writeValueAsBytes(fileMetadata));
                        //String valueStr = new String(value, StandardCharsets.UTF_8);
    //                    PhysicalMetadata metadata = objectMapper.readValue(value, PhysicalMetadata.class);
                        //VersionIndexMetadata metadata = objectMapper.readValue(value, VersionIndexMetadata.class);
                        //metadata.setFileName("/413930_12321_3a97a1b1e83fb65dd12134f8d55d3ae55955ac71_3wHWnasGntnMQtYdw8WeTgNxkg0HQRo0");
    //                    logger.info("value: {}", metadata.getFileName());
    //                    metadata.setFileName("/413930_12321_3a97a1b1e83fb65dd12134f8d55d3ae55955ac71_3wHWnasGntnMQtYdw8WeTgNxkg0HQRo0");
    //                    String newValueStr = objectMapper.writeValueAsString(metadata);
    //                    logger.info("data: {}", metadata.getFileName());
    //                    //String newKey = "#413930_12321_3a97a1b1e83fb65dd12134f8d55d3ae55955ac71_3wHWnasGntnMQtYdw8WeTgNxkg0HQRo0";
                        // String versionMetadataKey = "*4436/12321/testfile.txt" + new String(new byte[]{0}) + "null";
    //                    String lifeCycleMetaKey = "+4436/12321/testfile.txt" + "/null";
    //                    String latestMetaKey = "-4436/12321/testfile.txt";
    //                    String metaKey = "4436/12321/testfile.txt";
    //                    LatestIndexMetadata indexMetadata = VersionIndexMetadata.toIndexMetadata(metadata);
    //                    String newValueStr = objectMapper.writeValueAsString(metadata);
                        //String newMinusValueStr = objectMapper.writeValueAsString(metadata);
    //                    db.put(metaKey.getBytes(StandardCharsets.UTF_8), newValueStr.getBytes(StandardCharsets.UTF_8));
    //                    db.put(latestMetaKey.getBytes(StandardCharsets.UTF_8), newMinusValueStr.getBytes(StandardCharsets.UTF_8));
    //                    db.delete(lifeCycleMetaKey.getBytes(StandardCharsets.UTF_8));
                        //db.put(versionMetadataKey.getBytes(StandardCharsets.UTF_8), newMinusValueStr.getBytes(StandardCharsets.UTF_8));
                        //db.put(lifeCycleMetaKey.getBytes(StandardCharsets.UTF_8), new byte[SPACE_LEN]);
                       // System.out.println(new String(value, StandardCharsets.UTF_8));
              //      }


                } catch (RocksDBException e) {
                    logger.error("操作 RocksDB 时出错: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            logger.error("初始化 RocksDB 时出错: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
