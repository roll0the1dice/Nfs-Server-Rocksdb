package com.mycompany.rocksdb.utils;

import com.mycompany.rocksdb.SnowflakeIdGenerator;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

public class VersionUtil {
    private static SnowflakeIdGenerator snowflakeIdGenerator = new SnowflakeIdGenerator(2, 1);
    private static Tuple3<String, String, AtomicLong> VERSION = Tuples.of(new String(String.valueOf(snowflakeIdGenerator.nextId() / 10000_0000L)), new String(String.valueOf(snowflakeIdGenerator.nextId() / 10000_0000L)), new AtomicLong(100_0000L));

    public static String getVersionNumTrue() {
        Tuple3<String, String, AtomicLong> tmp = VERSION;

        if (null == tmp || null == tmp.getT1()) {
            throw new IllegalArgumentException("version time is not init");
        }

        return (tmp.getT1() + System.currentTimeMillis()) + "-" + tmp.getT3().incrementAndGet();
    }

    public static long newInode() {
        Tuple3<String, String, AtomicLong> tmp = VERSION;

        if (null == tmp || null == tmp.getT1()) {
            throw new IllegalArgumentException("version time is not init");
        }

        long nodeId = Long.parseLong(tmp.getT1()) * 10000_0000L + tmp.getT3().incrementAndGet();
        long num = 0;
        while (num++ < 8192) {
            nodeId = Long.parseLong(tmp.getT1()) * 10000_0000L + tmp.getT3().incrementAndGet();
        }

        return nodeId;
    }

    public static void main(String[] args) {
        System.out.println(VersionUtil.getVersionNumTrue());
    }

}
