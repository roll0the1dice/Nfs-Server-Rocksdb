package com.mycompany.rocksdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.rocksdb.netserver.Nfsv3Server;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.UnicastProcessor;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class RequestChannalHandler {
        private UnicastProcessor<Payload> responseFlux;
        // 只在本流内维护状态
        private final List<byte[]> chunkBuffer = new ArrayList<>();
        private String fileName;

        public RequestChannalHandler(UnicastProcessor<Payload> responseFlux) {
            this.responseFlux = responseFlux;
        }

        // 迁移自SimpleRSocketServer.java的handle*方法和内部类
        // 依赖字段
        private static final String STORAGE_DIR = "storage";
        private final ConcurrentHashMap<String, FileUploadSession> activeSessions = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, FileDownloadSession> activeDownloadSessions = new ConcurrentHashMap<>();

        // handleStartUpload
        public void handleStartUpload(Payload payload) {
            try {
                String metadata = payload.getDataUtf8();
                ObjectMapper objectMapper = new ObjectMapper();
                SocketReqMsg socketReqMsg = objectMapper.readValue(metadata, SocketReqMsg.class);
                String fileName = socketReqMsg.get("filename");
                FileUploadSession session = new FileUploadSession(fileName);
                activeSessions.put(fileName, session);
                responseFlux.onNext(DefaultPayload.create("Upload started", "CONTINUE"));
            } catch (Exception e) {
                responseFlux.onNext(DefaultPayload.create("Error: " + e.getMessage(), "ERROR"));
            }
        }

        // handleDataBlock
        public void handleDataBlock(Payload payload) {
            try {
                io.netty.buffer.ByteBuf byteBuf = payload.sliceData();
                int dataSize = byteBuf.readableBytes();
                byte[] data = new byte[dataSize];
                byteBuf.readBytes(data);
                if (activeSessions.isEmpty()) {
                    responseFlux.onNext(DefaultPayload.create("No active session", "ERROR"));
                    return;
                }
                FileUploadSession session = activeSessions.values().iterator().next();
                session.addData(data);
                responseFlux.onNext(DefaultPayload.create("Data received", "CONTINUE"));
            } catch (Exception e) {
                responseFlux.onNext(DefaultPayload.create("Error: " + e.getMessage(), "ERROR"));
            }
        }

        // handleCompleteUpload
        public void handleCompleteUpload(io.rsocket.Payload payload) {
            try {
                String metadata = payload.getDataUtf8();
                ObjectMapper objectMapper = new ObjectMapper();
                SocketReqMsg socketReqMsg = objectMapper.readValue(metadata, SocketReqMsg.class);
                String fileName = socketReqMsg.get("filename");
                String verisonKey = socketReqMsg.get("verisonKey");
                long count = Long.parseLong(socketReqMsg.get("count"));
                boolean isCreated = Boolean.getBoolean(socketReqMsg.get("isCreated"));


                if (activeSessions.isEmpty()) {
                    responseFlux.onNext(DefaultPayload.create("No active session", "ERROR"));
                    responseFlux.onComplete();
                    return;
                }
                FileUploadSession session = activeSessions.values().iterator().next();
                String etag = session.complete(fileName, verisonKey, count, isCreated);
                activeSessions.remove(session.getFileName());
                responseFlux.onNext(DefaultPayload.create(etag, "SUCCESS"));
                responseFlux.onComplete();
            } catch (Exception e) {
                responseFlux.onNext(DefaultPayload.create("Error: " + e.getMessage(), "ERROR"));
                responseFlux.onComplete();
            }
        }

        // handleStartDownload
        public void handleStartDownload(Payload payload) {
            try {
                String metadata = payload.getDataUtf8();
                com.fasterxml.jackson.databind.ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();
                SocketReqMsg socketReqMsg = objectMapper.readValue(metadata, SocketReqMsg.class);
                String fileName = socketReqMsg.get("filename");
                Path filePath = Paths.get(STORAGE_DIR, fileName);
                if (Files.exists(filePath)) {
                    FileDownloadSession session = new FileDownloadSession(fileName, filePath);
                    activeDownloadSessions.put(fileName, session);
                    responseFlux.onNext(DefaultPayload.create("Download started", "START_GET_OBJECT"));
                } else {
                    responseFlux.onNext(DefaultPayload.create("File not found: " + fileName, "ERROR"));
                    responseFlux.onComplete();
                }
            } catch (Exception e) {
                responseFlux.onNext(DefaultPayload.create("Error: " + e.getMessage(), "ERROR"));
            }
        }

        // handleDownloadChunk
        public void handleDownloadChunk(Payload payload) {
            try {
                String chunkIndex = payload.getDataUtf8();
                if (activeDownloadSessions.isEmpty()) {
                    responseFlux.onNext(DefaultPayload.create("No active download session".getBytes(), "ERROR".getBytes()));
                    responseFlux.onComplete();
                    return;
                }
                FileDownloadSession session = activeDownloadSessions.values().iterator().next();
                byte[] chunk = session.getNextChunk();
                if (chunk != null) {
                    responseFlux.onNext(DefaultPayload.create(chunk, "CONTINUE".getBytes()));
                } else {
                    activeDownloadSessions.remove(session.getFileName());
                    responseFlux.onNext(DefaultPayload.create("Download completed".getBytes(), "SUCCESS".getBytes()));
                    responseFlux.onComplete();
                }
            } catch (Exception e) {
                responseFlux.onNext(DefaultPayload.create(("Error: " + e.getMessage()).getBytes(), "ERROR".getBytes()));
                responseFlux.onComplete();
            }
        }

        // handleCompleteDownload
        public void handleCompleteDownload(Payload payload) {
            try {
                if (!activeDownloadSessions.isEmpty()) {
                    FileDownloadSession session = activeDownloadSessions.values().iterator().next();
                    activeDownloadSessions.remove(session.getFileName());
                }
                responseFlux.onNext(DefaultPayload.create("Download completed".getBytes(), "SUCCESS".getBytes()));
                responseFlux.onComplete();
            } catch (Exception e) {
                responseFlux.onNext(DefaultPayload.create(("Error: " + e.getMessage()).getBytes(), "ERROR".getBytes()));
                responseFlux.onComplete();
            }
        }

        // FileUploadSession内部类
        private static class FileUploadSession {
            private final String fileName;
            private final List<byte[]> dataBuffer = new ArrayList<>();
            private final java.util.concurrent.atomic.AtomicLong totalSize = new java.util.concurrent.atomic.AtomicLong(0);
            private final java.security.MessageDigest digest;
            public FileUploadSession(String fileName) {
                this.fileName = fileName;
                try {
                    this.digest = java.security.MessageDigest.getInstance("MD5");
                } catch (java.security.NoSuchAlgorithmException e) {
                    throw new RuntimeException("MD5 not available", e);
                }
            }
            public void addData(byte[] data) {
                dataBuffer.add(data);
                totalSize.addAndGet(data.length);
                digest.update(data);
            }
            public String complete(String fileName, String verisonKey, long count, boolean isCreated) throws java.io.IOException {
                ByteBuffer buffer = ByteBuffer.allocate(Math.toIntExact(totalSize.get()));
                for (byte[] array : dataBuffer) {
                    buffer.put(array);
                }

                Nfsv3Server.getMyRocksDB().saveFileMetaData(fileName, verisonKey, buffer.array(), count, isCreated);
                byte[] hash = digest.digest();
                return bytesToHex(hash);
            }
            public String getFileName() { return fileName; }
            public long getTotalSize() { return totalSize.get(); }
        }
        // FileDownloadSession内部类
        private static class FileDownloadSession {
            private final String fileName;
            private final java.nio.file.Path filePath;
            private final java.io.RandomAccessFile randomAccessFile;
            private final long fileSize;
            private final int chunkSize = 64 * 1024; // 64KB chunks
            private long currentPosition = 0;
            public FileDownloadSession(String fileName, java.nio.file.Path filePath) throws java.io.IOException {
                this.fileName = fileName;
                this.filePath = filePath;
                this.randomAccessFile = new java.io.RandomAccessFile(filePath.toFile(), "r");
                this.fileSize = randomAccessFile.length();
            }
            public byte[] getNextChunk() throws java.io.IOException {
                if (currentPosition >= fileSize) {
                    randomAccessFile.close();
                    return null;
                }
                long remainingBytes = fileSize - currentPosition;
                int bytesToRead = (int) Math.min(chunkSize, remainingBytes);
                byte[] chunk = new byte[bytesToRead];
                randomAccessFile.seek(currentPosition);
                randomAccessFile.read(chunk);
                currentPosition += bytesToRead;
                return chunk;
            }
            public String getFileName() { return fileName; }
            public long getFileSize() { return fileSize; }
            public long getCurrentPosition() { return currentPosition; }
        }
        // 工具方法
        private long bytesToLong(byte[] bytes) {
            long value = 0;
            for (int i = 0; i < 8; i++) {
                value = (value << 8) | (bytes[i] & 0xFF);
            }
            return value;
        }
        private String calculateMD5(byte[] data) {
            try {
                java.security.MessageDigest digest = java.security.MessageDigest.getInstance("MD5");
                byte[] hash = digest.digest(data);
                return bytesToHex(hash);
            } catch (java.security.NoSuchAlgorithmException e) {
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
}
