package com.mycompany.rocksdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.rocksdb.netserver.Nfsv3Server;
import com.mycompany.rocksdb.utils.MetaKeyUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.*;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 简单的RSocket文件服务器
 * 支持小文件优化上传和大文件分块处理
 */
@Slf4j
public class SimpleRSocketServer extends AbstractRSocket {

    // 存储目录
    private static final String STORAGE_DIR = "storage";
    // 小文件阈值 (128KB)
    private static final int SMALL_FILE_THRESHOLD = 128 * 1024;
    // 最小分配大小 (4KB)
    private static final int MIN_ALLOC_SIZE = 4 * 1024;

    // 活跃的上传会话
    private final ConcurrentHashMap<String, FileUploadSession> activeSessions = new ConcurrentHashMap<>();
    // 活跃的下载会话
    private final ConcurrentHashMap<String, FileDownloadSession> activeDownloadSessions = new ConcurrentHashMap<>();

    public enum PayloadMetaType {
        START_PUT_OBJECT,      // 开始上传
        PUT_OBJECT,           // 数据块
        COMPLETE_PUT_OBJECT,  // 完成上传
        PUT_AND_COMPLETE_PUT_OBJECT, // 小文件一次性上传
        GET_OBJECT,           // 获取文件
        START_GET_OBJECT,     // 开始流式下载
        GET_OBJECT_CHUNK,     // 获取文件块
        COMPLETE_GET_OBJECT,  // 完成流式下载
        DELETE_OBJECT,        // 删除文件
        SUCCESS,              // 成功响应
        ERROR,                // 错误响应
        CONTINUE,              // 继续响应
        PUT_METADATA,
        PUT_REDIS_DATA
    }

    public SimpleRSocketServer() {
        // 创建存储目录
        createStorageDirectory();
    }
    /**
     * 处理请求-响应模式
     */
    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        try {
            String metaType = payload.getMetadataUtf8();
            PayloadMetaType type = PayloadMetaType.valueOf(metaType);

            switch (type) {
                case PUT_AND_COMPLETE_PUT_OBJECT:
                    return handleSmallFileUpload(payload);
                case DELETE_OBJECT:
                    return handleDeleteObject(payload);
                case GET_OBJECT:
                    return handleGetObject(payload);
                case PUT_METADATA:
                    return handlePutMetadata(payload);
                case PUT_REDIS_DATA:
                    return handlePutRedisdata(payload);
                default:
                    return Mono.just(DefaultPayload.create("Unsupported operation", PayloadMetaType.ERROR.name()));
            }
        } catch (Exception e) {
            log.error("Request response error", e);
            return Mono.just(DefaultPayload.create("Error: " + e.getMessage(), PayloadMetaType.ERROR.name()));
        }
    }

    /**
     * 处理流式请求（大文件上传和下载）
     */
    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        UnicastProcessor<Payload> requestFlux = UnicastProcessor.create(Queues.<Payload>unboundedMultiproducer().get());
        UnicastProcessor<Payload> responseFlux = UnicastProcessor.create(Queues.<Payload>unboundedMultiproducer().get());
        AioUploadServerHandler uploadServerHandler = new AioUploadServerHandler(responseFlux);

        Flux.from(payloads).subscribe(payload -> {
            try {
                String metaType = payload.getMetadataUtf8();
                PayloadMetaType type = PayloadMetaType.valueOf(metaType);


                switch (type) {
                    case PUT_AND_COMPLETE_PUT_OBJECT:
                        handleSmallFileInStream(payload, requestFlux, responseFlux);
                        break;
                    case START_PUT_OBJECT:
                        uploadServerHandler.handleStartUpload(payload);
                        break;
                    case PUT_OBJECT:
                        //handleDataBlock(payload, requestFlux, responseFlux);
                        uploadServerHandler.handleDataBlock(payload);
                        break;
                    case COMPLETE_PUT_OBJECT:
                        //handleCompleteUpload(payload, requestFlux, responseFlux);
                        uploadServerHandler.handleCompleteUpload(payload);
                        break;
                    case START_GET_OBJECT:
                        //handleStartDownload(payload, requestFlux, responseFlux);
                        uploadServerHandler.handleStartDownload(payload);
                        break;
                    case GET_OBJECT_CHUNK:
                        uploadServerHandler.handleDownloadChunk(payload);
                        break;
                    case COMPLETE_GET_OBJECT:
                        uploadServerHandler.handleCompleteDownload(payload);
                        break;
                    default:
                        responseFlux.onNext(DefaultPayload.create("Unsupported operation", PayloadMetaType.ERROR.name()));
                        responseFlux.onComplete();
                }
            } catch (Exception e) {
                log.error("Request channel error", e);
                responseFlux.onNext(DefaultPayload.create("Error: " + e.getMessage(), PayloadMetaType.ERROR.name()));
                responseFlux.onComplete();
            }
        });

        return responseFlux;
    }

    /**
     * 处理小文件一次性上传（请求-响应模式）
     */
    private Mono<Payload> handleSmallFileUpload(Payload payload) {
        try {
            // 解析合并的请求
            ByteBuf byteBuf = payload.sliceData();
            byte[] lenBytes = new byte[8];
            byteBuf.getBytes(0, lenBytes);
            int startLen = (int) bytesToLong(lenBytes);

            byte[] startBytes = new byte[startLen];
            byteBuf.getBytes(8, startBytes);
            String metadata = new String(startBytes);

            int lastLen = byteBuf.readableBytes() - startBytes.length - 8;
            byte[] fileData = new byte[lastLen];
            byteBuf.getBytes(startBytes.length + 8, fileData);

            // 解析元数据
            //String fileName = extractFileName(metadata);
            ObjectMapper objectMapper = new ObjectMapper();
            SocketReqMsg socketReqMsg = objectMapper.readValue(startBytes, SocketReqMsg.class);
            String fileName = socketReqMsg.get("filename");
            String filePath = STORAGE_DIR + File.separator + fileName;

            // 写入文件
            Files.write(Paths.get(filePath), fileData);

            // 计算MD5
            String etag = calculateMD5(fileData);

            log.info("Small file uploaded: {}, size: {}, etag: {}", fileName, fileData.length, etag);

            return Mono.just(DefaultPayload.create(etag, PayloadMetaType.SUCCESS.name()));

        } catch (Exception e) {
            log.error("Small file upload error", e);
            return Mono.just(DefaultPayload.create("Upload failed: " + e.getMessage(), PayloadMetaType.ERROR.name()));
        }
    }

    /**
     * 处理小文件流式上传
     */
    private void handleSmallFileInStream(Payload payload, UnicastProcessor<Payload> requestFlux, UnicastProcessor<Payload> responseFlux) {
        try {
            // 解析合并的请求
            ByteBuf byteBuf = payload.sliceData();
            byte[] lenBytes = new byte[8];
            byteBuf.getBytes(0, lenBytes);
            int startLen = (int) bytesToLong(lenBytes);

            byte[] startBytes = new byte[startLen];
            byteBuf.getBytes(8, startBytes);
            ObjectMapper objectMapper = new ObjectMapper();
            SocketReqMsg socketReqMsg = objectMapper.readValue(startBytes, SocketReqMsg.class);

            int lastLen = byteBuf.readableBytes() - startBytes.length - 8;
            byte[] fileData = new byte[lastLen];
            byteBuf.getBytes(startBytes.length + 8, fileData);

            // 重构为标准流程
            requestFlux.onNext(DefaultPayload.create(startBytes, PayloadMetaType.START_PUT_OBJECT.name().getBytes()));
            requestFlux.onNext(DefaultPayload.create(fileData, PayloadMetaType.PUT_OBJECT.name().getBytes()));
            requestFlux.onNext(DefaultPayload.create("", PayloadMetaType.COMPLETE_PUT_OBJECT.name()));
            requestFlux.onComplete();

            // 创建上传会话
            //String fileName = socketReqMsg.get("filename");
            //FileUploadSession session = new FileUploadSession(fileName);
            //activeSessions.put(fileName, session);

        } catch (Exception e) {
            log.error("Small file stream error", e);
            responseFlux.onNext(DefaultPayload.create("Error: " + e.getMessage(), PayloadMetaType.ERROR.name()));
            responseFlux.onComplete();
        }
    }

    /**
     * 处理元数据上传
     */
    private Mono<Payload> handlePutRedisdata(Payload payload) {
        try {
            String metadata = payload.getDataUtf8();
            ObjectMapper objectMapper = new ObjectMapper();
            SocketReqMsg socketReqMsg = objectMapper.readValue(metadata, SocketReqMsg.class);
            String bucket = socketReqMsg.get("bucket");
            String object = socketReqMsg.get("object");
            String objectVnodeId = MetaKeyUtils.getObjectVnodeId(bucket, object);
            List<Long> link = Arrays.asList(((long) Long.parseLong(objectVnodeId)));
            String s_uuid = "0001";

            Nfsv3Server.getMyRocksDB().saveRedis(objectVnodeId, link, s_uuid);
            log.info("Metadata stored: {}", metadata);
            return Mono.just(DefaultPayload.create("Metadata stored", PayloadMetaType.SUCCESS.name()));
        } catch (Exception e) {
            log.error("Put metadata error", e);
            return Mono.just(DefaultPayload.create("Put metadata failed: " + e.getMessage(), PayloadMetaType.ERROR.name()));
        }
    }

    /**
     * 处理文件删除
     */
    private Mono<Payload> handleDeleteObject(Payload payload) {
        try {
            String metadata = payload.getDataUtf8();
            //String fileName = extractFileName(metadata);
            ObjectMapper objectMapper = new ObjectMapper();
            SocketReqMsg socketReqMsg = objectMapper.readValue(metadata, SocketReqMsg.class);
            String fileName = socketReqMsg.get("filename");
            Path filePath = Paths.get(STORAGE_DIR, fileName);

            if (Files.exists(filePath)) {
                Files.delete(filePath);
                log.info("File deleted: {}", fileName);
                return Mono.just(DefaultPayload.create("File deleted", PayloadMetaType.SUCCESS.name()));
            } else {
                return Mono.just(DefaultPayload.create("File not found", PayloadMetaType.ERROR.name()));
            }
        } catch (Exception e) {
            log.error("Delete file error", e);
            return Mono.just(DefaultPayload.create("Delete failed: " + e.getMessage(), PayloadMetaType.ERROR.name()));
        }
    }

    /**
     * 处理文件获取
     */
    private Mono<Payload> handleGetObject(Payload payload) {
        try {
            String metadata = payload.getDataUtf8();
            //String fileName = extractFileName(metadata);
            ObjectMapper objectMapper = new ObjectMapper();
            SocketReqMsg socketReqMsg = objectMapper.readValue(metadata, SocketReqMsg.class);
            String fileName = socketReqMsg.get("filename");
            Path filePath = Paths.get(STORAGE_DIR, fileName);

            if (Files.exists(filePath)) {
                byte[] fileData = Files.readAllBytes(filePath);
                String etag = calculateMD5(fileData);

                log.info("File retrieved: {}, size: {}, etag: {}", fileName, fileData.length, etag);
                return Mono.just(DefaultPayload.create(fileData, etag.getBytes()));
            } else {
                return Mono.just(DefaultPayload.create("File not found", PayloadMetaType.ERROR.name()));
            }
        } catch (Exception e) {
            log.error("Get file error", e);
            return Mono.just(DefaultPayload.create("Get failed: " + e.getMessage(), PayloadMetaType.ERROR.name()));
        }
    }

    /**
     * 处理元数据上传
     */
    private Mono<Payload> handlePutMetadata(Payload payload) {
        try {
            String metadata = payload.getDataUtf8();
            //String fileName = extractFileName(metadata);
            ObjectMapper objectMapper = new ObjectMapper();
            SocketReqMsg socketReqMsg = objectMapper.readValue(metadata, SocketReqMsg.class);
            String targetVnodeId = socketReqMsg.get("targetVnodeId");
            String bucket = socketReqMsg.get("bucket");
            String object = socketReqMsg.get("object");
            String fileName = socketReqMsg.get("fileName");
            String contentLength = socketReqMsg.get("contentLength");
            String contentType = socketReqMsg.get("contentType");
            Boolean isCreated = Boolean.valueOf(socketReqMsg.get("isCreated"));

            System.out.println("fileName: " + fileName);

            // 这里可以根据实际需求将元数据存储到文件、数据库或内存等
            // 示例：简单写入到一个本地文件
            Nfsv3Server.getMyRocksDB().saveIndexMetaData(targetVnodeId, bucket, object, fileName, Long.parseLong(contentLength), contentType, isCreated);
            log.info("IndexMetadata stored: {}", metadata);
            return Mono.just(DefaultPayload.create("Metadata stored", PayloadMetaType.SUCCESS.name()));
        } catch (Exception e) {
            log.error("Put metadata error", e);
            return Mono.just(DefaultPayload.create("Put metadata failed: " + e.getMessage(), PayloadMetaType.ERROR.name()));
        }
    }

    /**
     * 文件上传会话
     */
    private static class FileUploadSession {
        private final String fileName;
        private final StringBuilder dataBuffer = new StringBuilder();
        private final AtomicLong totalSize = new AtomicLong(0);
        private final MessageDigest digest;

        public FileUploadSession(String fileName) {
            this.fileName = fileName;
            try {
                this.digest = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("MD5 not available", e);
            }
        }

        public void addData(byte[] data) {
            dataBuffer.append(new String(data));
            totalSize.addAndGet(data.length);
            digest.update(data);
        }

        public String complete() throws IOException {
            String filePath = STORAGE_DIR + File.separator + fileName;
            Files.write(Paths.get(filePath), dataBuffer.toString().getBytes());

            byte[] hash = digest.digest();
            return bytesToHex(hash);
        }

        public String getFileName() {
            return fileName;
        }

        public long getTotalSize() {
            return totalSize.get();
        }
    }

    /**
     * 文件下载会话
     */
    private static class FileDownloadSession {
        private final String fileName;
        private final Path filePath;
        private final java.io.RandomAccessFile randomAccessFile;
        private final long fileSize;
        private final int chunkSize = 64 * 1024; // 64KB chunks
        private long currentPosition = 0;

        public FileDownloadSession(String fileName, Path filePath) throws IOException {
            this.fileName = fileName;
            this.filePath = filePath;
            this.randomAccessFile = new java.io.RandomAccessFile(filePath.toFile(), "r");
            this.fileSize = randomAccessFile.length();
        }

        public byte[] getNextChunk() throws IOException {
            if (currentPosition >= fileSize) {
                randomAccessFile.close();
                return null; // 文件读取完成
            }

            long remainingBytes = fileSize - currentPosition;
            int bytesToRead = (int) Math.min(chunkSize, remainingBytes);

            byte[] chunk = new byte[bytesToRead];
            randomAccessFile.seek(currentPosition);
            randomAccessFile.read(chunk);

            currentPosition += bytesToRead;

            return chunk;
        }

        public String getFileName() {
            return fileName;
        }

        public long getFileSize() {
            return fileSize;
        }

        public long getCurrentPosition() {
            return currentPosition;
        }
    }

    // 工具方法
    private void createStorageDirectory() {
        try {
            Files.createDirectories(Paths.get(STORAGE_DIR));
        } catch (IOException e) {
            log.error("Failed to create storage directory", e);
        }
    }

    private String extractFileName(String metadata) {
        // 简化的元数据解析，实际应该使用JSON
        if (metadata.contains("fileName:")) {
            return metadata.split("fileName:")[1].split(",")[0];
        }
        return "default_" + System.currentTimeMillis() + ".txt";
    }

    private long bytesToLong(byte[] bytes) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = (value << 8) | (bytes[i] & 0xFF);
        }
        return value;
    }

    private String calculateMD5(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] hash = digest.digest(data);
            return bytesToHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not available", e);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /**
     * 启动服务器
     */
    public static void main(String[] args) {
        SimpleRSocketServer server = new SimpleRSocketServer();

        CloseableChannel channel = RSocketFactory.receive()
                .frameDecoder(PayloadDecoder.DEFAULT)
                .acceptor((setup, sendingSocket) -> Mono.just(server))
                .transport(TcpServerTransport.create("0.0.0.0", 7000))
                .start()
                .block();

        log.info("Simple RSocket Server started on 0.0.0.0:7000 (accessible from any IP)");

        // 保持服务器运行
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            log.info("Server interrupted");
        } finally {
            if (channel != null) {
                channel.dispose();
            }
        }
    }
}
