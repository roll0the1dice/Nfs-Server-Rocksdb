package com.mycompany.rocksdb.utils;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import reactor.util.function.Tuple2;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import static com.mycompany.rocksdb.constant.GlobalConstant.VNODE_NUM;

public class MetaKeyUtils {
    private static final AtomicLong globalCounter = new AtomicLong(10000000);

    public static final String ZERO_STR;
    public static final String ROCKS_VERSION_PREFIX = "*";
    public static final String ROCKS_LATEST_KEY = "-";
    public static final String ROCKS_LIFE_CYCLE_PREFIX = "+";
    public static final String ROCKS_FILE_META_PREFIX = "#";

    static {
        ZERO_STR = new String(new byte[]{0});
    }

    // 生成版本号对应的rocksDB的key
    // *vnode1/mybucket/example.txt/0000000000000/version1
    public static String getVersionMetaDataKey(String vnode, String bucket, String object, String versionId) {
        if (StringUtils.isEmpty(versionId)) {
            return getMetaDataKey(vnode, bucket, object, null);
        }
        return ROCKS_VERSION_PREFIX + vnode + File.separator + bucket + File.separator + object + ZERO_STR + versionId;
    }

    // -vnode1/mybucket/example.txt
    public static String getLatestMetaKey(String vnode, String bucket, String object) {

        return ROCKS_LATEST_KEY + vnode + File.separator + bucket + File.separator + object;
    }

    // 生成生命周期对应的rocksDB的stamp的key
    // +vnode1/mybucket/1234567890/example.txt/version1
    public static String getLifeCycleMetaKey(String vnode, String bucket, String object, String versionId, String stamp) {
        return ROCKS_LIFE_CYCLE_PREFIX + vnode + File.separator + bucket + File.separator + stamp + File.separator + object + File.separator + versionId;
    }

    // 不带快照标记时：vnode1/mybucket/example.txt
    // 带快照标记时：vnode1/mybucket/snapshotMark/example.txt
    // 带版本号时：vnode1/mybucket/example.txt/0000000000000/versionId
    public static String getMetaDataKey(String vnode, String bucket, String object, String snapshotMark) {
        return StringUtils.isBlank(snapshotMark) ? vnode + File.separator + bucket + File.separator + object
                : vnode + File.separator + bucket + File.separator + snapshotMark + File.separator + object;
    }

    public static String getMetaDataKey(String vnode, String bucket, String object, String versionId, String stamp) {
        return vnode + File.separator + bucket + File.separator + object + ZERO_STR + stamp + File.separator + versionId;
    }

    public static String getMetaDataKey(String vnode, String bucket, String object, String versionId, String stamp, String snapshotMark) {
        return StringUtils.isBlank(snapshotMark) ? getMetaDataKey(vnode, bucket, object, versionId, stamp)
                : vnode + File.separator + bucket + File.separator + snapshotMark + File.separator + object + ZERO_STR + File.separator + stamp + File.separator + versionId;
    }

    public static String getObjFileName(String bucket, String object, String requestId) {
        Tuple2<String, String> tuple = MsVnodeUtils.getObjectVnodeId(bucket, object, VNODE_NUM);

        String objVnode = tuple.getT1();
        String sha1 = tuple.getT2();

        return File.separator + String.join("_",
                new String[]{objVnode,
                        bucket,
                        sha1,
                        requestId});
    }

    public static String getObjectVnodeId(String bucket, String object) {
        return MsVnodeUtils.getObjectVnodeId(bucket, object, VNODE_NUM).getT1();
        //return "4436";
    }

    public static String getFileMetaKey(String fileName) {
        return ROCKS_FILE_META_PREFIX + fileName.split(File.separator)[1];
    }

    public static String getRequestId() {
        // 如果lang3的版本大于3.14，且操作系统为centos，这个地方会变得很慢
        return RandomStringUtils.randomAlphanumeric(32);
    }

    public static String getVersionNum() {
        long counter = globalCounter.incrementAndGet();
        long timestamp = System.currentTimeMillis();
        return String.format("%019d%d-%d", counter, timestamp, 1000001).trim();
    }

    public static String getshardingStamp() {
        long counter = globalCounter.incrementAndGet();
        long timestamp = System.currentTimeMillis();
        return String.format("%019d%d-%d", counter, timestamp, 1000002).trim();
    }

    public static String getTargetVnodeId(String bucketName) {
        return MsVnodeUtils.getTargetVnodeId(bucketName, VNODE_NUM);
    }

    public static String getHoleFileName(String bucket, String requestId) {
        Tuple2<String, String> tuple = MsVnodeUtils.getObjectVnodeId(bucket, "", VNODE_NUM);

        String objVnode = tuple.getT1();

        return File.separator + String.join("_",
                new String[]{objVnode,
                        bucket,
                        "",
                        requestId});
    }

}
