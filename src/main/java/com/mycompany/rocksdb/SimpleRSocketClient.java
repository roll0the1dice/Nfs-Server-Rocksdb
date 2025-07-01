package com.mycompany.rocksdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 简单的RSocket文件客户端
 * 用于测试文件上传功能
 */
@Slf4j
public class SimpleRSocketClient {

    private final RSocket rsocket;
    private final String serverHost;
    private final int serverPort;

    public SimpleRSocketClient(String host, int port) {
        this.serverHost = host;
        this.serverPort = port;
        log.info("Connecting to RSocket server at {}:{}", host, port);

        this.rsocket = RSocketFactory.connect()
                .transport(TcpClientTransport.create(host, port))
                .start()
                .block();

        if (this.rsocket == null) {
            throw new RuntimeException("Failed to connect to RSocket server at " + host + ":" + port);
        }

        log.info("Successfully connected to RSocket server at {}:{}", host, port);
    }

    /**
     * 小文件一次性上传（请求-响应模式）
     */
    public Mono<String> uploadSmallFile(String filePath, String fileName) {
        try {
            byte[] fileData = Files.readAllBytes(Paths.get(filePath));

            SocketReqMsg msg = new SocketReqMsg("", 0)
                    .put("bucket", "12321")
                    .put("object", "test.txt")
                    .put("filename", fileName)
                    .put("filePath", filePath);
            ObjectMapper objectMapper = new ObjectMapper();
            byte[] metadataBytes = objectMapper.writeValueAsBytes(msg);
            // 构建元数据
//            String metadata = "fileName:" + fileName + ",size:" + fileData.length;
//            byte[] metadataBytes = metadata.getBytes();

            // 构建合并的payload
            byte[] lenBytes = longToBytes(metadataBytes.length);
            byte[] combinedData = new byte[8 + metadataBytes.length + fileData.length];

            System.arraycopy(lenBytes, 0, combinedData, 0, 8);
            System.arraycopy(metadataBytes, 0, combinedData, 8, metadataBytes.length);
            System.arraycopy(fileData, 0, combinedData, 8 + metadataBytes.length, fileData.length);

            return rsocket.requestResponse(DefaultPayload.create(combinedData,
                            SimpleRSocketServer.PayloadMetaType.PUT_AND_COMPLETE_PUT_OBJECT.name().getBytes()))
                    .map(payload -> payload.getDataUtf8());

        } catch (IOException e) {
            log.error("Failed to read file: {}", filePath, e);
            return Mono.error(e);
        }
    }


    /**
     * 上传元数据（请求-响应模式）
     */
    public Mono<String> putMetadata(String metadata) {
        return rsocket.requestResponse(
                DefaultPayload.create(metadata.getBytes(), SimpleRSocketServer.PayloadMetaType.PUT_METADATA.name().getBytes())
        ).map(payload -> payload.getDataUtf8());
    }

    /**
     * 上传元数据（请求-响应模式）
     */
    public Mono<String> putMetadata(SocketReqMsg msg) {
        ObjectMapper objectMapper = new ObjectMapper();
        byte[] metadata = null;
        try {
            metadata = objectMapper.writeValueAsBytes(msg);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return rsocket.requestResponse(
                DefaultPayload.create(metadata, SimpleRSocketServer.PayloadMetaType.PUT_METADATA.name().getBytes())
        ).map(payload -> payload.getDataUtf8());
    }

    /**
     * 上传元数据（请求-响应模式）
     */
    public Mono<String> putRedisdata(SocketReqMsg msg) {
        ObjectMapper objectMapper = new ObjectMapper();
        byte[] metadata = null;
        try {
            metadata = objectMapper.writeValueAsBytes(msg);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return rsocket.requestResponse(
                DefaultPayload.create(metadata, SimpleRSocketServer.PayloadMetaType.PUT_REDIS_DATA.name().getBytes())
        ).map(payload -> payload.getDataUtf8());
    }


    /**
     * 大文件分块上传（流式模式，InputStream分块读取）
     */
    public Mono<String> uploadLargeFile(String filePath, String filename) {
        try {
            // 构建元数据
            Path path = Paths.get(filePath);
            ObjectMapper objectMapper = new ObjectMapper();
            SocketReqMsg msg = new SocketReqMsg("", 0)
                    .put("filename", filename);
            byte[] metadataBytes = objectMapper.writeValueAsBytes(msg);
            int chunkSize = 64 * 1024; // 64KB

            InputStream inputStream = Files.newInputStream(path);
            BufferedInputStream bis = new BufferedInputStream(inputStream);

            AtomicLong counter = new AtomicLong(0);

            Flux<io.rsocket.Payload> payloadFlux = Flux.concat(
                    // 发送开始请求
                    Mono.just(DefaultPayload.create(metadataBytes,
                            SimpleRSocketServer.PayloadMetaType.START_PUT_OBJECT.name().getBytes())),
                    // 发送数据块
                    Flux.generate(
                            () -> bis,
                            (stream, sink) -> {
                                try {
                                    byte[] buffer = new byte[chunkSize];
                                    int read = stream.read(buffer);
                                    if (read == -1) {
                                        stream.close();
                                        sink.complete();
                                    } else {
                                        byte[] chunk = (read == chunkSize) ? buffer : java.util.Arrays.copyOf(buffer, read);
                                        counter.addAndGet(1);
                                        log.info("send chunk {} --------", counter.get());
                                        sink.next(DefaultPayload.create(chunk,
                                                SimpleRSocketServer.PayloadMetaType.PUT_OBJECT.name().getBytes()));
                                    }
                                } catch (IOException e) {
                                    sink.error(e);
                                }
                                return stream;
                            }
                    ),

                    // 发送完成请求
                    Mono.just(DefaultPayload.create("".getBytes(),
                            SimpleRSocketServer.PayloadMetaType.COMPLETE_PUT_OBJECT.name().getBytes()))
            );

            return rsocket.requestChannel(payloadFlux)
                    .last()
                    .map(payload -> payload.getDataUtf8());

        } catch (IOException e) {
            log.error("Failed to read file: {}", filePath, e);
            return Mono.error(e);
        }
    }

    /**
     * 大文件分块上传（流式模式，InputStream分块读取）
     */
    public Mono<String> uploadLargeFile(String filePath, SocketReqMsg msg) {
        try {
            // 构建元数据
            Path path = Paths.get(filePath);
            ObjectMapper objectMapper = new ObjectMapper();
            byte[] metadataBytes = objectMapper.writeValueAsBytes(msg);
            int chunkSize = 64 * 1024; // 64KB

            InputStream inputStream = Files.newInputStream(path);
            BufferedInputStream bis = new BufferedInputStream(inputStream);

            AtomicLong counter = new AtomicLong(0);

            Flux<io.rsocket.Payload> payloadFlux = Flux.concat(
                    // 发送开始请求
                    Mono.just(DefaultPayload.create(metadataBytes,
                            SimpleRSocketServer.PayloadMetaType.START_PUT_OBJECT.name().getBytes())),
                    // 发送数据块
                    Flux.generate(
                            () -> bis,
                            (stream, sink) -> {
                                try {
                                    byte[] buffer = new byte[chunkSize];
                                    int read = stream.read(buffer);
                                    if (read == -1) {
                                        stream.close();
                                        sink.complete();
                                    } else {
                                        byte[] chunk = (read == chunkSize) ? buffer : java.util.Arrays.copyOf(buffer, read);
                                        counter.addAndGet(1);
                                        log.info("send chunk {}, chunk_size {} -------- ", counter.get(), chunk.length);
                                        sink.next(DefaultPayload.create(chunk,
                                                SimpleRSocketServer.PayloadMetaType.PUT_OBJECT.name().getBytes()));
                                    }
                                } catch (IOException e) {
                                    sink.error(e);
                                }
                                return stream;
                            }
                    ),

                    // 发送完成请求
                    Mono.just(DefaultPayload.create(metadataBytes,
                            SimpleRSocketServer.PayloadMetaType.COMPLETE_PUT_OBJECT.name().getBytes()))
            );

            return rsocket.requestChannel(payloadFlux)
                    .last()
                    .map(payload -> payload.getDataUtf8());

        } catch (IOException e) {
            log.error("Failed to read file: {}", filePath, e);
            return Mono.error(e);
        }
    }

    /**
     * 下载文件
     */
    public Mono<byte[]> downloadFile(String fileName) {
        try {
            SocketReqMsg msg = new SocketReqMsg("", 0)
                    .put("bucket", "12321")
                    .put("object", "test.txt")
                    .put("filename", fileName);
            ObjectMapper objectMapper = new ObjectMapper();
            byte[] metadataBytes = objectMapper.writeValueAsBytes(msg);
            return rsocket.requestResponse(DefaultPayload.create(metadataBytes,
                            SimpleRSocketServer.PayloadMetaType.GET_OBJECT.name().getBytes()))
                    .map(payload -> {
                        java.nio.ByteBuffer data = payload.getData();
                        byte[] fileData = new byte[data.remaining()];
                        data.get(fileData);
                        return fileData;
                    });
        } catch (RuntimeException e) {
            throw new RuntimeException(e);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 大文件流式下载（分块下载）
     */
    public Mono<String> downloadLargeFile(String fileName, String localFilePath) {
        try {
            // 构建下载请求元数据
//            String metadata = "fileName:" + fileName + ",chunkSize:65536"; // 64KB chunks
            SocketReqMsg msg = new SocketReqMsg("", 0)
                    .put("bucket", "12321")
                    .put("object", "test.txt")
                    .put("filename", fileName)
                    .put("filePath", localFilePath);
            ObjectMapper objectMapper = new ObjectMapper();
            byte[] metadataBytes = objectMapper.writeValueAsBytes(msg);

            // 创建本地文件输出流
            java.io.FileOutputStream fos = new java.io.FileOutputStream(localFilePath);
            java.io.BufferedOutputStream bos = new java.io.BufferedOutputStream(fos);

            // 使用有限请求，最多1000个块（约64MB）
            Flux<io.rsocket.Payload> requestFlux = Flux.concat(
                    // 发送开始下载请求
                    Mono.just(DefaultPayload.create(metadataBytes,
                            SimpleRSocketServer.PayloadMetaType.START_GET_OBJECT.name().getBytes())),

                    // 发送有限数量的分块请求
                    Flux.range(0, 1000) // 最多1000个块
                            .map(i -> DefaultPayload.create(String.valueOf(i).getBytes(),
                                    SimpleRSocketServer.PayloadMetaType.GET_OBJECT_CHUNK.name().getBytes())),

                    // 发送完成请求
                    Mono.just(DefaultPayload.create("".getBytes(),
                            SimpleRSocketServer.PayloadMetaType.COMPLETE_GET_OBJECT.name().getBytes()))
            );

            return rsocket.requestChannel(requestFlux)
                    .doOnNext(payload -> {
                        String status = payload.getMetadataUtf8();
                        log.debug("Received response with status: {}", status);

                        if ("CONTINUE".equals(status)) {
                            // 接收数据块
                            java.nio.ByteBuffer data = payload.getData();
                            byte[] chunk = new byte[data.remaining()];
                            data.get(chunk);

                            try {
                                bos.write(chunk);
                                log.debug("Received chunk: {} bytes", chunk.length);
                            } catch (IOException e) {
                                log.error("Failed to write chunk to file", e);
                            }
                        } else if ("SUCCESS".equals(status)) {
                            // 下载完成
                            try {
                                bos.close();
                                fos.close();
                                log.info("File download completed: {}", localFilePath);
                            } catch (IOException e) {
                                log.error("Failed to close file", e);
                            }
                        } else if ("ERROR".equals(status)) {
                            // 下载错误
                            log.error("Download error: {}", payload.getDataUtf8());
                            try {
                                bos.close();
                                fos.close();
                            } catch (IOException e) {
                                log.error("Failed to close file on error", e);
                            }
                        }
                        // 忽略其他状态的消息（如 "Download started"）
                    })
                    .last()
                    .map(payload -> {
                        if ("SUCCESS".equals(payload.getMetadataUtf8())) {
                            return "Download completed: " + localFilePath;
                        } else {
                            return "Download failed: " + payload.getDataUtf8();
                        }
                    })
                    .doOnError(error -> {
                        log.error("Download stream error", error);
                        try {
                            bos.close();
                            fos.close();
                        } catch (IOException e) {
                            log.error("Failed to close file on error", e);
                        }
                    });

        } catch (IOException e) {
            log.error("Failed to create output file: {}", localFilePath, e);
            return Mono.error(e);
        }
    }

    /**
     * 带进度监控的流式下载
     */
    public Mono<String> downloadLargeFileWithProgress(String fileName, String localFilePath) {
        try {
            String metadata = "fileName:" + fileName + ",chunkSize:65536";

            // 创建本地文件输出流
            java.io.FileOutputStream fos = new java.io.FileOutputStream(localFilePath);
            java.io.BufferedOutputStream bos = new java.io.BufferedOutputStream(fos);

            // 进度跟踪
            AtomicLong totalBytes = new AtomicLong(0);
            AtomicLong chunkCount = new AtomicLong(0);

            // 使用有限请求，最多1000个块（约64MB）
            Flux<io.rsocket.Payload> requestFlux = Flux.concat(
                    Mono.just(DefaultPayload.create(metadata.getBytes(),
                            SimpleRSocketServer.PayloadMetaType.START_GET_OBJECT.name().getBytes())),

                    Flux.range(0, 1000) // 最多1000个块
                            .map(i -> DefaultPayload.create(String.valueOf(i).getBytes(),
                                    SimpleRSocketServer.PayloadMetaType.GET_OBJECT_CHUNK.name().getBytes())),

                    // 发送完成请求
                    Mono.just(DefaultPayload.create("".getBytes(),
                            SimpleRSocketServer.PayloadMetaType.COMPLETE_GET_OBJECT.name().getBytes()))
            );

            return rsocket.requestChannel(requestFlux)
                    .doOnNext(payload -> {
                        String status = payload.getMetadataUtf8();
                        log.debug("Received response with status: {}", status);

                        if ("CONTINUE".equals(status)) {
                            java.nio.ByteBuffer data = payload.getData();
                            byte[] chunk = new byte[data.remaining()];
                            data.get(chunk);
                            try {
                                bos.write(chunk);
                                long bytes = totalBytes.addAndGet(chunk.length);
                                long chunks = chunkCount.incrementAndGet();

                                // 每10个块显示一次进度
                                if (chunks % 10 == 0) {
                                    log.info("Download progress: {} chunks, {} bytes", chunks, bytes);
                                }
                            } catch (IOException e) {
                                log.error("Failed to write chunk to file", e);
                            }
                        } else if ("SUCCESS".equals(status)) {
                            try {
                                bos.close();
                                fos.close();
                                log.info("Download completed: {} chunks, {} bytes",
                                        chunkCount.get(), totalBytes.get());
                            } catch (IOException e) {
                                log.error("Failed to close file", e);
                            }
                        } else if ("ERROR".equals(status)) {
                            log.error("Download error: {}", payload.getDataUtf8());
                            try {
                                bos.close();
                                fos.close();
                            } catch (IOException e) {
                                log.error("Failed to close file on error", e);
                            }
                        }
                        // 忽略其他状态的消息（如 "Download started"）
                    })
                    .last()
                    .map(payload -> {
                        if ("SUCCESS".equals(payload.getMetadataUtf8())) {
                            return String.format("Download completed: %s (%d bytes)",
                                    localFilePath, totalBytes.get());
                        } else {
                            return "Download failed: " + payload.getDataUtf8();
                        }
                    })
                    .doOnError(error -> {
                        log.error("Download stream error", error);
                        try {
                            bos.close();
                            fos.close();
                        } catch (IOException e) {
                            log.error("Failed to close file on error", e);
                        }
                    });

        } catch (IOException e) {
            log.error("Failed to create output file: {}", localFilePath, e);
            return Mono.error(e);
        }
    }

    /**
     * 删除文件
     */
    public Mono<String> deleteFile(String fileName) {
        try {
            SocketReqMsg msg = new SocketReqMsg("", 0)
                    .put("bucket", "12321")
                    .put("object", "test.txt")
                    .put("filename", fileName);
            ObjectMapper objectMapper = new ObjectMapper();
            byte[] metadataBytes = objectMapper.writeValueAsBytes(msg);
            return rsocket.requestResponse(DefaultPayload.create(metadataBytes,
                            SimpleRSocketServer.PayloadMetaType.DELETE_OBJECT.name().getBytes()))
                    .map(payload -> payload.getDataUtf8());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 计算文件MD5
     */
    private String calculateMD5(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] hash = digest.digest(data);
            return bytesToHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not available", e);
        }
    }

    /**
     * 工具方法：long转字节数组
     */
    private byte[] longToBytes(long value) {
        byte[] bytes = new byte[8];
        for (int i = 7; i >= 0; i--) {
            bytes[i] = (byte) (value & 0xFF);
            value >>= 8;
        }
        return bytes;
    }

    /**
     * 工具方法：字节数组转十六进制字符串
     */
    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /**
     * 关闭连接
     */
    public void dispose() {
        if (rsocket != null) {
            rsocket.dispose();
        }
    }

    /**
     * 测试方法
     */
    public static void main(String[] args) {
        // 默认连接参数
        String host = "172.20.123.123";
        int port = 7000;

        // 解析命令行参数
        if (args.length >= 1) {
            host = args[0];
        }
        if (args.length >= 2) {
            try {
                port = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                log.error("Invalid port number: {}", args[1]);
                System.exit(1);
            }
        }

        log.info("Starting RSocket client, connecting to {}:{}", host, port);

        SimpleRSocketClient client = null;
        try {
            client = new SimpleRSocketClient(host, port);
            final SimpleRSocketClient finalClient = client;

            // 创建测试文件
            String testContent = "Hello, RSocket! This is a test file for the simple file server.";
            Path testFile = Paths.get("test.txt");
            Files.write(testFile, testContent.getBytes());

            log.info("Testing small file upload...");
//            finalClient.uploadSmallFile("test.txt", "test_small.txt")
//                    .subscribe(etag -> {
//                        log.info("Small file uploaded successfully, ETag: {}", etag);
//
//                        // 下载文件验证
//                        log.info("Downloading file to verify...");
//                        finalClient.downloadFile("test_small.txt")
//                                .subscribe(data -> {
//                                    String downloadedContent = new String(data);
//                                    log.info("Downloaded content: {}", downloadedContent);
//                                    log.info("Content matches: {}", testContent.equals(downloadedContent));
//
//                                    // 删除文件
//                                    finalClient.deleteFile("test_small.txt")
//                                            .subscribe(result -> log.info("File deleted: {}", result));
//                                });
//                    });

            // 测试大文件上传
            log.info("Testing large file upload...");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 1000; i++) { // 减少到1000次，约18KB
                sb.append("Large file content ");
            }
            String largeContent = sb.toString();
            Path largeFile = Paths.get("large_test.txt");
            Files.write(largeFile, largeContent.getBytes());

            finalClient.uploadLargeFile("large_test.txt", "test_large.txt")
                    .subscribe(etag -> {
                        log.info("Large file uploaded successfully, size:{}, ETag: {}", largeContent.length(), etag);

                        // 测试流式下载
                        log.info("Testing large file download...");
                        finalClient.downloadLargeFile("test_large.txt", "downloaded_large.txt")
                                .subscribe(result -> {
                                    log.info("Large file download result: {}", result);

                                    // 验证下载的文件
                                    try {
                                        byte[] originalData = Files.readAllBytes(largeFile);
                                        byte[] downloadedData = Files.readAllBytes(Paths.get("downloaded_large.txt"));
                                        boolean matches = java.util.Arrays.equals(originalData, downloadedData);
                                        log.info("Downloaded file matches original: {}", matches);
                                        log.info("Original file size: {} bytes", originalData.length);
                                        log.info("Downloaded file size: {} bytes", downloadedData.length);
                                    } catch (IOException e) {
                                        log.error("Failed to verify downloaded file", e);
                                    }
                                }, error -> {
                                    log.error("Download failed", error);
                                });

                        // 清理测试文件
//                        try {
//                            Files.deleteIfExists(testFile);
//                            Files.deleteIfExists(largeFile);
//                        } catch (IOException e) {
//                            log.error("Failed to cleanup test files", e);
//                        }
                    }, error -> {
                        log.error("Upload failed", error);
                    });

            // 等待一段时间让操作完成
            Thread.sleep(10000); // 增加到10秒

        } catch (Exception e) {
            log.error("Test failed", e);
            e.printStackTrace();
        } finally {
            if (client != null) {
                log.info("Disposing client connection...");
                client.dispose();
            }
        }
    }
}

