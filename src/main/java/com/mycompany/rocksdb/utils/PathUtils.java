package com.mycompany.rocksdb.utils;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 一个用于处理文件路径的实用工具类。
 */
public final class PathUtils {

    /**
     * 将一个目录路径和一个文件名（或相对路径）合并成一个规范化的、合法的全路径。
     *
     * 这个方法可以正确处理不同操作系统（Windows/Linux/macOS）的路径分隔符，
     * 并能解析路径中的 "." (当前目录) 和 ".." (上级目录)。
     *
     * @param directory 父目录的路径。如果为 null 或空，则直接返回文件名。
     * @param filename  文件名或相对于父目录的路径。
     * @return 合并并规范化后的全路径字符串。
     */
    public static String combine(String directory, String filename) {
        // --- 处理边界情况 ---
        if (filename == null) {
            // 如果文件名是 null，没有意义，可以返回目录或抛出异常
            return directory; 
        }
        if (directory == null || directory.trim().isEmpty()) {
            // 如果目录是空的，那么路径就是文件名本身
            return Paths.get(filename).normalize().toString();
        }

        // --- 核心逻辑 ---
        // 1. 使用 Paths.get() 来安全地合并路径。它会自动处理路径分隔符。
        Path combinedPath = Paths.get(directory, filename);
        
        // 2. 使用 .normalize() 来处理 ".." 和 "."，生成一个干净的路径。
        Path normalizedPath = combinedPath.normalize();

        // 3. 返回路径的字符串表示形式。
        return normalizedPath.toString();
    }
}