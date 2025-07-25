package com.mycompany.rocksdb;

import com.mycompany.rocksdb.netserver.Nfsv3Server;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static com.mycompany.rocksdb.utils.NetTool.bytesToHex2;

public class MD5Util {

    /**
     * 计算字节数组的MD5值。
     *
     * @param data 要计算的字节数组
     * @return 32位小写的MD5字符串；如果输入为null或发生错误，则返回null
     */
    public static String getMd5(byte[] data) {
        // 1. 输入校验
        if (data == null) {
            return null;
        }

        try {
            // 2. 获取MessageDigest实例，指定算法为MD5
            MessageDigest md = MessageDigest.getInstance("MD5");

            // 3. 计算哈希值
            // digest(byte[]) 方法是 update(byte[]) 和 digest() 的一个快捷方式
            byte[] digestBytes = md.digest(data);

            // 4. 将计算结果（字节数组）转换为十六进制字符串
            return bytesToHexString(digestBytes);

        } catch (NoSuchAlgorithmException e) {
            // "MD5"算法在所有标准Java平台都支持，所以这里几乎不可能发生
            System.err.println("MD5 algorithm not found!");
            // 在生产代码中，这里应该使用日志框架记录错误
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 辅助方法：将字节数组转换为十六进制字符串。
     *
     * @param bytes 字节数组
     * @return 小写的十六进制字符串
     */
    private static String bytesToHexString(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        StringBuilder hexString = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            // String.format("%02x", b) 可将字节格式化为2位小写十六进制数
            // %x -> 十六进制
            // 02 -> 宽度为2，不足时前面补0
            hexString.append(String.format("%x", b));
        }
        return hexString.toString().trim();
    }

    public static String calculateMD5(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] hash = digest.digest(data);
            return bytesToHex2(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not available", e);
        }
    }


    // --- main方法用于演示 ---
    public static void main(String[] args) {
        // 示例：计算字符串 "hello world" 对应字节数组的MD5值
        String originalString = "hello world";

        // 将字符串按UTF-8编码转换为字节数组
        byte[] bytesToHash = originalString.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        System.out.println("Original String: \"" + originalString + "\"");
        System.out.println("Bytes to hash (UTF-8): " + java.util.Arrays.toString(bytesToHash));

        // 调用我们的方法计算MD5
        String md5Hash = calculateMD5(bytesToHash);

        System.out.println("MD5 Hash: " + md5Hash);
        //          5eb63bbbe01eeed093cb22bb8f5acdc3
        //          5eb63bbbe01eeed093cb22bb8f5acdc3
        //          5eb63bbbe01eeed093cb22bb8f5acdc3
    }
}
