package com.mycompany.rocksdb.smb2;

import com.mycompany.rocksdb.smb2.POJO.QueryDirectoryRequest;
import com.mycompany.rocksdb.smb2.POJO.QueryDirectoryResponse;
import com.mycompany.rocksdb.smb2.POJO.Smb2Header;
import io.vertx.reactivex.core.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class Smb2QueryDirectoryHandler implements Smb2OperationHandler {
    private static final Logger log = LoggerFactory.getLogger(Smb2QueryDirectoryHandler.class);
    private static final String SHARE_ROOT_PATH = "/tmp/smb2_share";

    @Override
    public Buffer handle(Smb2RequestContext context, Smb2Header requestHeader, int currentReqOffset) {
        int bodyOffset = Smb2Header.STRUCTURE_SIZE + currentReqOffset; // SMB2 header is 64 bytes

        try {
            QueryDirectoryRequest queryDirectoryRequest = QueryDirectoryRequest.decode(context.getSmb2Message(), bodyOffset);

            log.info("QUERY_DIRECTORY: FileInfoClass: 0x{}, Flags: 0x{}, FileId: {}/{}, SearchPattern: {}",
                    Long.toHexString(queryDirectoryRequest.getFileInformationClass()),
                    Long.toHexString(queryDirectoryRequest.getFlags()),
                    queryDirectoryRequest.getFileIdVolatile(),
                    queryDirectoryRequest.getFileIdPersistent(),
                    queryDirectoryRequest.getSearchPattern());

            Smb2ConnectionState state = context.getState();
            if (state.getSessionId() == 0) {
                log.warn("QUERY_DIRECTORY: No active session for client.");
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_USER_SESSION_NOT_FOUND);
            }

            if (state.getTreeConnectId() == 0) {
                log.warn("QUERY_DIRECTORY: No active tree connect for client.");
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_NETWORK_NAME_DELETED);
            }

            long fileId = queryDirectoryRequest.getFileIdPersistent() == -1L ? state.getCurrentFileHandle().getFileId() : queryDirectoryRequest.getFileIdPersistent();
            Smb2ConnectionState.Smb2FileHandle fileHandle = state.getFileHandle(fileId);
            if (fileHandle == null) {
                log.warn("QUERY_DIRECTORY: File handle not found for fileId: {}", fileId);
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_FILE_CLOSED);
            }

            // 1. 获取客户端的控制标志
            // SMB2_RESTART_SCANS (0x01): 强制从头开始
            boolean restartScan = (queryDirectoryRequest.getFlags() & 0x01) != 0;
            // SMB2_RETURN_SINGLE_ENTRY (0x02): 只返回一个条目
            boolean singleEntry = (queryDirectoryRequest.getFlags() & 0x02) != 0;

            if (restartScan) {
                fileHandle.setLastIndex(0L); 
            }

            Path directoryPath = Paths.get(fileHandle.getPath());
            if (!Files.isDirectory(directoryPath)) {
                log.warn("QUERY_DIRECTORY: Path is not a directory: {}", directoryPath);
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_NOT_A_DIRECTORY);
            }

            // 3. 读取目录并过滤
            Pattern pattern = null;
            if (queryDirectoryRequest.getSearchPattern() != null && !queryDirectoryRequest.getSearchPattern().isEmpty() 
                && !queryDirectoryRequest.getSearchPattern().equals("*")) {
                String regexPattern = queryDirectoryRequest.getSearchPattern()
                        .replace(".", "\\.")
                        .replace("*", ".*")
                        .replace("?", ".");
                pattern = Pattern.compile(regexPattern, Pattern.CASE_INSENSITIVE);
            }

            // 1. 用于存储已经构建并对齐的条目列表
            List<Buffer> entryList = new ArrayList<>();
            int totalSerializedLength = 0;
            int maxOutputLength = queryDirectoryRequest.getOutputBufferLength();
            long currentIndex = fileHandle.getLastIndex();
            log.info("QUERY_DIRECTORY: Starting enumeration from index {}", currentIndex);

            try (DirectoryStream<Path> stream = Files.newDirectoryStream(directoryPath)) {
                for (Path entry : stream) {
                    String fileName = entry.getFileName().toString();
                    
                    // 过滤模式
                    if (pattern != null && !pattern.matcher(fileName).matches()) {
                        continue;
                    }

                    // 构建单个条目（此时 NextEntryOffset 先填 0）
                    Buffer entryBuffer = buildDirectoryEntry(entry, queryDirectoryRequest.getFileInformationClass());
                    
                    if (entryBuffer != null) {
                        int currentLen = entryBuffer.length();
                        // 计算 8 字节对齐所需的填充
                        int padding = (8 - (currentLen % 8)) % 8;
                        int alignedLen = currentLen + padding;

                        // 检查是否超出客户端请求的缓冲区大小
                        if (totalSerializedLength + alignedLen > maxOutputLength) {
                            // 如果这是第一个条目就塞不下，返回错误
                            if (entryList.isEmpty()) {
                                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_INFO_LENGTH_MISMATCH);
                            }
                            // 否则，停止枚举，返回已有的条目
                            break; 
                        }

                        // 执行填充
                        if (padding > 0) {
                            entryBuffer.appendBytes(new byte[padding]);
                        }

                        entryList.add(entryBuffer);
                        totalSerializedLength += alignedLen;
                    }
                }
            } catch (IOException e) {
                log.error("QUERY_DIRECTORY: IO Error listing {}", directoryPath, e);
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_ACCESS_DENIED);
            }

            // 2. 检查结果集是否为空（非常重要：解决 Input/Output error）
            if (entryList.isEmpty()) {
                // 如果一个文件都没找到，返回 STATUS_NO_SUCH_FILE (0xC000000F)
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_NO_SUCH_FILE);
            }

            // 3. 串联条目并设置 NextEntryOffset
            Buffer responseData = Buffer.buffer();
            for (int i = 0; i < entryList.size(); i++) {
                Buffer current = entryList.get(i);
                
                if (i < entryList.size() - 1) {
                    // 如果不是最后一个条目，设置 NextEntryOffset 为当前条目到下一个条目的距离（即对齐后的长度）
                    current.setIntLE(0, current.length()); 
                } else {
                    // 最后一个条目的 NextEntryOffset 必须为 0
                    current.setIntLE(0, 0);
                }
                responseData.appendBuffer(current);

                currentIndex++; 
                if (singleEntry) break;
            }

            if (currentIndex > entryList.size()) {
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_NO_MORE_FILES);
            }

            // 发送完数据后，标记该句柄已经枚举过
            fileHandle.setLastIndex(currentIndex);

            // --- 5. 手动构建响应体，确保 StructureSize 是 9 ---
            QueryDirectoryResponse queryDirectoryResponse = new QueryDirectoryResponse();
            queryDirectoryResponse.setStructureSize((short) 9);
            queryDirectoryResponse.setOutputBufferOffset((short) 72); // Header(64) + Body(8)
            queryDirectoryResponse.setOutputBufferLength(responseData.length());
            queryDirectoryResponse.setBufferData(responseData.getBytes());
            Buffer bodyBuffer = queryDirectoryResponse.encode();

            // --- 6. 构建最终 Buffer ---
            Smb2Header responseHeader = new Smb2Header();
            responseHeader.setCommand((short) Smb2Constants.SMB2_QUERY_DIRECTORY);
            responseHeader.setStatus(Smb2Constants.STATUS_SUCCESS);
            responseHeader.setFlags(Smb2Constants.SMB2_FLAG_RESPONSE);
            responseHeader.setMessageId(requestHeader.getMessageId());
            responseHeader.setCreditRequestResponse((short) 1);
            responseHeader.setSessionId(state.getSessionId());
            responseHeader.setTreeId(state.getTreeConnectId());

            Buffer fullResponse = Buffer.buffer();
            fullResponse.appendBuffer(responseHeader.encode());
            fullResponse.appendBuffer(bodyBuffer);

            return fullResponse;

        } catch (Exception e) {
            log.error("Error handling SMB2 QUERY_DIRECTORY command: ", e);
            return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_INTERNAL_ERROR);
        }
    }

    private Buffer buildDirectoryEntry(Path entryPath, byte fileInformationClass) throws IOException {
        String fileName = entryPath.getFileName().toString();
        byte[] fileNameBytes = fileName.getBytes("UTF-16LE");
        BasicFileAttributes attrs = Files.readAttributes(entryPath, BasicFileAttributes.class);

        // 时间转换 (100ns units since 1601)
        long creationTime = attrs.creationTime().toMillis() * 10000L + Smb2Constants.FILETIME_EPOCH_DIFF;
        long lastAccessTime = attrs.lastAccessTime().toMillis() * 10000L + Smb2Constants.FILETIME_EPOCH_DIFF;
        long lastWriteTime = attrs.lastModifiedTime().toMillis() * 10000L + Smb2Constants.FILETIME_EPOCH_DIFF;
        long changeTime = lastWriteTime;

        int fileAttributes = Files.isDirectory(entryPath) ? 0x10 : 0x80; // Directory or Normal

        Buffer b = Buffer.buffer();
        // 先写占位符 NextEntryOffset (4 bytes)，由外部循环根据对齐后的长度填充
        b.appendIntLE(0); 
        b.appendIntLE(0); // FileIndex

        switch (fileInformationClass) {
            case Smb2Constants.FILE_ID_BOTH_DIRECTORY_INFORMATION: // 0x26 (38)
                b.appendLongLE(creationTime);
                b.appendLongLE(lastAccessTime);
                b.appendLongLE(lastWriteTime);
                b.appendLongLE(changeTime);
                b.appendLongLE(attrs.size()); // EndOfFile
                b.appendLongLE(attrs.size()); // AllocationSize
                b.appendIntLE(fileAttributes);
                b.appendIntLE(fileNameBytes.length);
                b.appendIntLE(0); // EaSize
                b.appendIntLE(0); // Reserved
                b.appendLongLE(entryPath.hashCode()); // FileId (持久化ID)
                b.appendBytes(fileNameBytes);
                break;

            case Smb2Constants.FILE_DIRECTORY_INFORMATION: // 0x01
                b.appendLongLE(creationTime);
                b.appendLongLE(lastAccessTime);
                b.appendLongLE(lastWriteTime);
                b.appendLongLE(changeTime);
                b.appendLongLE(attrs.size());
                b.appendLongLE(attrs.size());
                b.appendIntLE(fileAttributes);
                b.appendIntLE(fileNameBytes.length);
                b.appendBytes(fileNameBytes);
                break;
            
            case Smb2Constants.FILE_FULL_DIRECTORY_INFORMATION: // 0x02
                // 8-32: 时间戳 (32 bytes)
                b.appendLongLE(lastAccessTime);
                b.appendLongLE(lastAccessTime);
                b.appendLongLE(lastWriteTime);
                b.appendLongLE(changeTime);
                
                // 40: EndOfFile (8 bytes)
                b.appendLongLE(attrs.size());
                // 48: AllocationSize (8 bytes)
                b.appendLongLE(attrs.size());
                
                // 56: FileAttributes (4 bytes)
                b.appendIntLE(fileAttributes);
                
                // 60: FileNameLength (4 bytes)
                b.appendIntLE(fileNameBytes.length);
                
                // 64: EaSize (4 bytes)
                b.appendIntLE(0); 
                
                // 68: FileName (Variable)
                b.appendBytes(fileNameBytes);
                break;
            // ... 其他 case 类似处理 ...
        }

        return b;
    }
}

