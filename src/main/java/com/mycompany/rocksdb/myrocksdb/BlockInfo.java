package com.mycompany.rocksdb.myrocksdb;

import static com.mycompany.rocksdb.constant.GlobalConstant.ROCKS_FILE_SYSTEM_PREFIX_OFFSET;

import lombok.Data;

@Data
public class BlockInfo {
    public String[] fileName;

    public long offset;

    public long[] len;

    public long total;

    public void setKey(String partKey) {

    }

    public String getKey() {
        return getKey(offset);
    }

    /**
     * PB
     */
    private static final int OFFSET_LEN = String.valueOf(1024L * 1024 * 1024 * 1024 * 1024).length();

    public static String getKey(long offset) {
        String offsetStr = String.valueOf(offset);
        if (offsetStr.length() < OFFSET_LEN) {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < OFFSET_LEN - offsetStr.length(); i++) {
                builder.append('0');
            }

            builder.append(offsetStr);
            return ROCKS_FILE_SYSTEM_PREFIX_OFFSET + builder.toString();
        } else {
            return ROCKS_FILE_SYSTEM_PREFIX_OFFSET + offset;
        }
    }

    public static String getFamilySpaceKey(long index) {
        StringBuilder sb = new StringBuilder();
        sb.append(".1.");
        String s = String.valueOf(index);
        for (int i = 0; i < 10 - s.length(); i++) {
            sb.append("0");
        }
        return sb.append(s).toString();
    }

    public static byte[] getBitArray(byte b) {
        byte[] array = new byte[8];
        for (int i = 7; i >= 0; i--) {
            array[i] = (byte) (b & 1);
            b = (byte) (b >> 1);
        }
        return array;
    }
}
