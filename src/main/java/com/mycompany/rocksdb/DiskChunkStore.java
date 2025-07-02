package com.mycompany.rocksdb;

import java.io.*;
import java.nio.file.Files;

public class DiskChunkStore implements AutoCloseable {

    private final File tempFile;
    private final FileOutputStream outputStream;

    public DiskChunkStore() throws IOException {
        // 创建一个临时文件来存储数据
        this.tempFile = Files.createTempFile("chunk-store-", ".tmp").toFile();
        // 如果JVM异常退出，请求系统在退出时删除文件
        this.tempFile.deleteOnExit();
        // 使用 BufferedOutputStream 提高写入性能
        this.outputStream = new FileOutputStream(this.tempFile);
    }

    /**
     * 将一个新的 byte[] 块追加到临时文件中
     *
     * @param dataChunk The data chunk to write.
     * @throws IOException if an I/O error occurs.
     */
    public void addChunk(byte[] dataChunk) throws IOException {
        if (dataChunk != null && dataChunk.length > 0) {
            outputStream.write(dataChunk);
        }
    }

    /**
     * 当所有数据都写入后，可以获取一个输入流来从头读取整个文件内容
     *
     * @return A new FileInputStream to read the stored data.
     * @throws IOException if the file is not found.
     */
    public InputStream getInputStreamForProcessing() throws IOException {
        // 确保所有缓冲的数据都已写入磁盘
        outputStream.flush();
        // 返回一个可以从头读取文件的新输入流
        return new FileInputStream(this.tempFile);
    }

    /**
     * 清理资源：关闭流并删除临时文件
     */
    @Override
    public void close() throws IOException {
        try {
            outputStream.close();
        } finally {
            // 确保临时文件被删除
            if (tempFile.exists()) {
                Files.delete(tempFile.toPath());
            }
        }
    }
}