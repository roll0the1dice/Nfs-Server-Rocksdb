package com.mycompany.rocksdb;

import org.rocksdb.ColumnFamilyHandle;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.io.FileInputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MountPointerResolver {
    /**
     * 请求长度较小时会从预留空间分配，一次分配的最小长度BLOCK_SIZE
     */
    public static final int BLOCK_SIZE = 4 * 1024;
    /**
     * 请求长度较大时，alloc一次分配的最小的长度MIN_ALLOC_SIZE
     */
    public static final int MIN_ALLOC_SIZE = 1024 * 1024;

    private static final byte[] BLOCK_DEVICE_START_BYTES = "MOSS_FS_fs-SP0-".getBytes();

    private static final int FS_NAME_LEN = "MOSS_FS_".getBytes().length;
    private static final byte[] OFFSET_MOVE_END_BYTES = "offset_move_end".getBytes();

    public static Map<String, BlockDevice> map = new ConcurrentHashMap<>();

    static class MountInfo {
        final String deviceName;
        final String mountDir;

        MountInfo(String deviceName, String mountDir) {
            this.deviceName = deviceName;
            this.mountDir = mountDir;
        }
    }

    private static Field findField(Class<?> clazz, String fieldName) {
        Class<?> current = clazz;
        while (current != null && current != Object.class) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                current = current.getSuperclass();
            }
        }
        return null;
    }

    public static MountInfo getMountInfo(FileStore store) {
        try {
            Field entryField = findField(store.getClass(), "entry");
            entryField.setAccessible(true);
            // 获取 UnixMountEntry 实例
            Object mountEntry = entryField.get(store);

            if (mountEntry == null) {
                return null;
            }

            String deviceName = getMountEntryProperty(mountEntry, "name");
            String mountDir = getMountEntryProperty(mountEntry, "dir");

            return new MountInfo(deviceName, mountDir);

        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getMountEntryProperty(Object mountEntry, String fieldName) throws IllegalAccessException, NoSuchFieldException {
        Field field = mountEntry.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        // byte[] value = mountEntry.filed
        byte[] value = (byte[]) field.get(mountEntry);

        return new String(value, StandardCharsets.UTF_8);
    }

    public static void main(String[] args) {
        try {

            List<MountInfo> mountInfoList = new ArrayList<>();

            for (FileStore store : FileSystems.getDefault().getFileStores()) {
                // 打印标准库提供的信息
//                System.out.println("Standard API -> name() " + store.name());
//                System.out.println("Standard API -> type() " + store.type());

                //System.out.println("Underlying Class: " + store.getClass().getName());

                MountInfo mountInfo = getMountInfo(store);

                if (mountInfo.deviceName.endsWith("1") || mountInfo.deviceName.endsWith("2")) {
                    mountInfoList.add(mountInfo);
                }

//                if (mountInfo != null) {
//                    System.out.println("Reflected Info -> Device Name (name): " + mountInfo.deviceName);
//                    System.out.println("Reflected Info -> Mount Directory (dir): " + mountInfo.mountDir);
//                } else {
//                    System.out.println("Could not resolve detailed mount info via reflection.");
//                }
            }

            mountInfoList.parallelStream()
                    .forEach(mountInfo1 -> {
                        String deviceName = mountInfo1.deviceName;
                        String mountDir = mountInfo1.mountDir;
                        String path = deviceName.substring(0, deviceName.length() - 1) + "3";

                        File file = new File(path);
                        if (!file.exists()) {
                            path = deviceName.substring(0, deviceName.length() - 1) + "2";
                            if (!new File(path).exists()) {
                                return;
                            }
                        }

                        System.out.println("path: " + path);
                        try (FileInputStream stream = new FileInputStream(path)) {
                            byte[] bytes = new byte[BLOCK_SIZE];
                            stream.read(bytes);
                            //System.out.println("Magic Number: " + new String(bytes, StandardCharsets.UTF_8));
                            boolean mossFS = true;
                            for (int i = 0; i < BLOCK_DEVICE_START_BYTES.length; i++) {
                                if (bytes[i] != BLOCK_DEVICE_START_BYTES[i]) {
                                    mossFS = false;
                                    break;
                                }
                            }

                            if (mossFS) {
                                int start = FS_NAME_LEN;
                                int end = start + 1;
                                while (!(bytes[end] == '\r' && bytes[end + 1] == '\n')) {
                                    end++;
                                }



                                String name = new String(bytes, start, end - start).replace(" ", "");
                                System.out.println("Magic Name: " + name);
                                if (!name.equals(mountDir.substring(1)) || map.containsKey(name)) {
                                    return;
                                }

                                start = end + 7;
                                end = start + 1;
                                while (!(bytes[end] == '\r' && bytes[end + 1] == '\n')) {
                                    end++;
                                }

                                String size = new String(bytes, start, end - start);
                                System.out.println("total size: " + size);
                                boolean offsetMoveEnd = true;
                                start = end + 2;
                                for (int i = 0; i < OFFSET_MOVE_END_BYTES.length; i++) {
                                    if (bytes[start + i] != OFFSET_MOVE_END_BYTES[i]) {
                                        offsetMoveEnd = false;
                                        break;
                                    }
                                }

                                System.out.println("offsetMoveEnd: " + offsetMoveEnd);

                                // redis 需要存在该lun，若不存在，则该lun无效
//                                String node = ServerConfig.getInstance().getHostUuid();
//                                long keyExists = RedisConnPool.getInstance().getCommand(REDIS_LUNINFO_INDEX).exists(node + "@" + name);
//                                if (keyExists == 0) {
//                                    return;
//                                }

                                //BlockDevice device = new BlockDevice(name, path, Long.parseLong(size), offsetMoveEnd);
                                //map.put(name, device);
                            }
                        } catch (Exception e) {
                            //log.error("", e);
                        }
                    });

        } catch (RuntimeException e) {
            throw new RuntimeException(e);
        }
    }

//    public static ColumnFamilyHandle getColumnFamily(String lun) {
//        return cfHandleMap.getOrDefault(lun, new HashMap<>()).get(ROCKS_FILE_SYSTEM_PREFIX_OFFSET);
//    }
//
//    private void tryInitBlockSpace(boolean isMoveEnd) {
//        try {
//            ColumnFamilyHandle columnFamilyHandle = MSRocksDB.getColumnFamily(name);
//            byte[] v = MSRocksDB.getRocksDB(name).get(columnFamilyHandle, BlockInfo.getFamilySpaceKey(0).getBytes());
//            if (null == v) {
//                if (isMoveEnd) {
//                    isMoveEnd = false;
//                    writeOffsetMoveFlag(false);
//                }
//                List<byte[]> keyList = new LinkedList<>();
//                byte[] value = new byte[SPACE_LEN];
//                Flux<Boolean> flux = Flux.empty();
//                for (int index = 0; index < size / SPACE_SIZE; index++) {
//                    keyList.add(BlockInfo.getFamilySpaceKey(index).getBytes());
//
//                    if (keyList.size() > 1000) {
//                        byte[][] keys = keyList.toArray(new byte[keyList.size()][]);
//                        flux = flux.mergeWith(BatchRocksDB.customizeOperateDataForInitBlockSpace(name, (db, w, r) -> {
//                            for (byte[] key : keys) {
//                                w.put(columnFamilyHandle, key, value);
//                            }
//                        }));
//
//                        keyList.clear();
//                    }
//                }
//
//                flux.collectList().block();
//                log.info("{} init block space ", name);
//            }
//            if (!isMoveEnd) {
//                scanMoveOffsetData();
//            }
//        } catch (Exception e) {
//            log.error("", e);
//            return;
//        }
//    }
}
