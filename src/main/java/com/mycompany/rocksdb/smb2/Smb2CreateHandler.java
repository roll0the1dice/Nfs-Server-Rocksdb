package com.mycompany.rocksdb.smb2;

import com.ctc.wstx.util.StringUtil;
import com.mycompany.rocksdb.smb2.POJO.CreateRequest;
import com.mycompany.rocksdb.smb2.POJO.CreateResponse;
import com.mycompany.rocksdb.smb2.POJO.Smb2Header;

import io.vertx.reactivex.core.buffer.Buffer;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.Optional;
import java.util.Random;

public class Smb2CreateHandler implements Smb2OperationHandler {
    private static final Logger log = LoggerFactory.getLogger(Smb2CreateHandler.class);
    private static final Random random = new Random();

    // Base directory for our simulated file system
    private static final String SHARE_ROOT_PATH = "/tmp/smb2_share";

    public Smb2CreateHandler() {
        // Ensure the share root directory exists
        try {
            Files.createDirectories(Paths.get(SHARE_ROOT_PATH));
            log.info("SMB2 share root directory created: {}", SHARE_ROOT_PATH);
        } catch (Exception e) {
            log.error("Failed to create SMB2 share root directory: {}", SHARE_ROOT_PATH, e);
        }
    }

    @Override
    public Buffer handle(Smb2RequestContext context, Smb2Header requestHeader, int currentReqOffset) {
        int bodyOffset = Smb2Header.STRUCTURE_SIZE + currentReqOffset; // SMB2 header is 64 bytes

        try {
            CreateRequest createRequest = CreateRequest.decode(context.getSmb2Message(), bodyOffset);
            String fileName = Optional.ofNullable(createRequest.getFileName()).orElse("");
            // if (fileName == null || fileName.isEmpty()) {
            //     log.warn("CREATE: No file could be found.");
            //     // 0xC00000C9: 网络名（共享）已删除或不可用
            //     return buildErrorResponse(requestHeader, Smb2Constants.STATUS_FILE_NOT_FOUND);
            // }
            Path filePath = Paths.get(SHARE_ROOT_PATH, fileName);

            log.info("CREATE: Client requested file: {}, DesiredAccess: 0x{}, CreateDisposition: 0x{}",
                    fileName,
                    Long.toHexString(createRequest.getDesiredAccess()),
                    Long.toHexString(createRequest.getCreateDisposition()));

            Smb2ConnectionState state = context.getState();
            if (state.getSessionId() == 0) {
                log.warn("CREATE: No active session for client.");
                // 0xC0000203: 会话不存在
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_USER_SESSION_NOT_FOUND);
            }

            if (state.getTreeConnectId() == 0) {
                log.warn("CREATE: No active tree connect for client.");
                // 0xC00000C9: 网络名（共享）已删除或不可用
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_NETWORK_NAME_DELETED);
            }

            long createAction = 0; // FILE_ACTION_ADDED, FILE_ACTION_OPENED, etc.
            long status = Smb2Constants.STATUS_SUCCESS;

            // Handle CreateDisposition
            switch ((int) createRequest.getCreateDisposition()) {
                case Smb2Constants.FILE_SUPERSEDE:
                    Files.deleteIfExists(filePath);
                    Files.createFile(filePath);
                    createAction = 0x00000003; // FILE_SUPERSEDED
                    break;
                case Smb2Constants.FILE_OPEN:
                    if (!Files.exists(filePath)) {
                        status = Smb2Constants.STATUS_FILE_NOT_FOUND;
                    } else {
                        createAction = 0x00000001; // FILE_OPENED
                    }
                    break;
                case Smb2Constants.FILE_CREATE:
                    if (Files.exists(filePath)) {
                        status = Smb2Constants.STATUS_OBJECT_NAME_COLLISION; // Or similar error
                    } else {
                        Files.createFile(filePath);
                        createAction = 0x00000002; // FILE_CREATED
                    }
                    break;
                case Smb2Constants.FILE_OPEN_IF:
                    if (Files.exists(filePath)) {
                        createAction = 0x00000001; // FILE_OPENED
                    } else {
                        Files.createFile(filePath);
                        createAction = 0x00000002; // FILE_CREATED
                    }
                    break;
                case Smb2Constants.FILE_OVERWRITE:
                    if (!Files.exists(filePath)) {
                        status = Smb2Constants.STATUS_FILE_NOT_FOUND;
                    } else {
                        Files.delete(filePath);
                        Files.createFile(filePath);
                        createAction = 0x00000004; // FILE_OVERWRITTEN
                    }
                    break;
                case Smb2Constants.FILE_OVERWRITE_IF:
                    if (Files.exists(filePath)) {
                        Files.delete(filePath);
                        Files.createFile(filePath);
                        createAction = 0x00000004; // FILE_OVERWRITTEN
                    } else {
                        Files.createFile(filePath);
                        createAction = 0x00000002; // FILE_CREATED
                    }
                    break;
                default:
                    log.warn("Unsupported CreateDisposition: 0x{}", Long.toHexString(createRequest.getCreateDisposition()));
                    status = Smb2Constants.STATUS_INVALID_PARAMETER;
                    break;
            }

            if (status != Smb2Constants.STATUS_SUCCESS) {
                return Smb2Utils.buildErrorResponse(requestHeader, status);
            }

            // Generate file ID and register it in connection state
            Smb2ConnectionState.Smb2FileHandle fileHandle = SHARE_ROOT_PATH.equals(filePath.toString()) ? state.putFileHandle(Smb2Server.ROOT_INODE.getInodeId(), SHARE_ROOT_PATH) : state.createNewFileHandle(filePath.toString());
            Smb2Server.openHandles.computeIfAbsent(fileHandle.getFileId(), k -> fileHandle.getPath());
            state.setCurrentFileHandle(fileHandle);

            // Construct the CREATE Response
            CreateResponse createResponse = new CreateResponse();
            createResponse.setStructureSize((short)0x0059);
            createResponse.setOplockLevel((byte) 0x00); // SMB2_OPLOCK_LEVEL_NONE
            createResponse.setCreateAction(createAction);

            // File times (using current time for simplicity)
            BasicFileAttributes attrs = Files.readAttributes(filePath, BasicFileAttributes.class);
            //currentTime = 0L; // For simplicity, set to 0
            createResponse.setCreationTime(Smb2Utils.toFileTime(attrs.creationTime()));
            createResponse.setLastAccessTime(Smb2Utils.toFileTime(attrs.lastAccessTime()));
            createResponse.setLastWriteTime(Smb2Utils.toFileTime(attrs.lastModifiedTime()));
            createResponse.setChangeTime(Smb2Utils.toFileTime(attrs.lastModifiedTime()));

            int attr = 0;
            if (attrs.isDirectory()) attr |= 0x10; // FILE_ATTRIBUTE_DIRECTORY
            else attr |= 0x80; // FILE_ATTRIBUTE_NORMAL
            if (!Files.isWritable(filePath)) attr |= 0x01; // READONLY

            createResponse.setFileAttributes(attr); // TODO: Set actual file attributes
            createResponse.setAllocationSize(attrs.size()); // TODO: Set actual allocation size
            createResponse.setEndOfFile(attrs.size());      // TODO: Set actual end of file

            createResponse.setFileIdVolatile((short) (fileHandle.getFileIdVolatile())); // Lower 16 bits
            createResponse.setFileIdPersistent(fileHandle.getFileId()); // Full 64-bit ID

            // Build the full SMB2 response including the header
            Smb2Header responseHeader = new Smb2Header();
            responseHeader.setCommand((short)Smb2Constants.SMB2_CREATE);
            responseHeader.setStatus(Smb2Constants.STATUS_SUCCESS);
            responseHeader.setFlags(Smb2Constants.SMB2_FLAG_RESPONSE);
            responseHeader.setMessageId(requestHeader.getMessageId());
            responseHeader.setCreditRequestResponse((short) 1);
            responseHeader.setSessionId(state.getSessionId());
            responseHeader.setTreeId(state.getTreeConnectId());

            Buffer responseBuffer = responseHeader.encode();
            Buffer bodyBuffer = createResponse.encode();

            // 4. 合并并添加 NBSS 长度头 (4字节，大端)
            Buffer fullPacket = Buffer.buffer();
            int totalBodySize = responseBuffer.length() + bodyBuffer.length();

            // fullPacket.appendByte((byte) 0x00);      // Type: Session Message
            // fullPacket.appendMedium(totalBodySize);   // 24位长度 (大端)
            fullPacket.appendBuffer(responseBuffer);
            fullPacket.appendBuffer(bodyBuffer);

            return fullPacket;

        } catch (Exception e) {
            log.error("Error handling SMB2 CREATE command: ", e);
            return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_INTERNAL_ERROR);
        }
    }
}

