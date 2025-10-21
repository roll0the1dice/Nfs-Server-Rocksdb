package com.mycompany.rocksdb.POJO;

public class CacheFlushConfigRefresher {
    public static final String CACHE_FLUSH_CONFIG_KEY_SUFFIX = "_flush_config";
    public static final String ENABLE_DELAYED_FLUSH_KEY = "enable_delayed_flush";
    public static final String ENABLE_ORDERED_FLUSH_KEY = "enable_ordered_flush";
    public static final String DELAYED_FLUSH_WATER_MARK_KEY = "delayed_flush_water_mark";
    public static final String LOW_WATER_MARK_KEY = "low";
    public static final String HIGH_WATER_MARK_KEY = "high";
    public static final String FULL_WATER_MARK_KEY = "full";

    public static final String DEFAULT_ENABLE_DELAYED_FLUSH = "false";
    public static final String DEFAULT_ENABLE_ORDERED_FLUSH = "false";
    public static final String DEFAULT_DELAYED_FLUSH_WATER_MARK = "20";

    public static final String DEFAULT_LOW_WATER_MARK;
    public static final String DEFAULT_HIGH_WATER_MARK;
    public static final String DEFAULT_FULL_WATER_MARK;


    static {
        DEFAULT_LOW_WATER_MARK = System.getProperty("com.macrosan.storage.cache.low", "40");
        DEFAULT_HIGH_WATER_MARK = System.getProperty("com.macrosan.storage.cache.high", "60");
        DEFAULT_FULL_WATER_MARK = System.getProperty("com.macrosan.storage.cache.full", "80");
    }

    private static final CacheFlushConfigRefresher INSTANCE = new CacheFlushConfigRefresher();


    public static CacheFlushConfigRefresher getInstance() {
        return INSTANCE;
    }
}
