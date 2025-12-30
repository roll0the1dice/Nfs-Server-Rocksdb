package com.mycompany.rocksdb.smb2;

import com.mycompany.rocksdb.smb2.POJO.CloseRequest;
import com.mycompany.rocksdb.smb2.POJO.CloseResponse;
import com.mycompany.rocksdb.smb2.POJO.Smb2Header;
import io.vertx.reactivex.core.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;

public class Smb2CloseHandler implements Smb2OperationHandler {
    private static final Logger log = LoggerFactory.getLogger(Smb2CloseHandler.class);

    @Override
    public Buffer handle(Smb2RequestContext context, Smb2Header requestHeader, int currentReqOffset) {
        int bodyOffset = Smb2Header.STRUCTURE_SIZE + currentReqOffset; // SMB2 header is 64 bytes

        try {
            CloseRequest closeRequest = CloseRequest.decode(context.getSmb2Message(), bodyOffset);

            log.info("CLOSE: FileId: {}/{}, Flags: 0x{}",
                    closeRequest.getFileIdVolatile(),
                    closeRequest.getFileIdPersistent(),
                    Long.toHexString(closeRequest.getFlags()));

            Smb2ConnectionState state = context.getState();
            if (state.getSessionId() == 0) {
                log.warn("CLOSE: No active session for client.");
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_USER_SESSION_NOT_FOUND);
            }

            if (state.getTreeConnectId() == 0) {
                log.warn("CLOSE: No active tree connect for client.");
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_NETWORK_NAME_DELETED);
            }

            long fileId = closeRequest.getFileIdPersistent();
            Smb2ConnectionState.Smb2FileHandle fileHandle = fileId == -1L ? state.getCurrentFileHandle() : state.getFileHandle(fileId);
            if (fileHandle == null) {
                log.warn("CLOSE: File handle not found for fileId: {}", fileId);
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_FILE_CLOSED);
            }

            // Remove the file handle from the connection state
            fileId = fileHandle.getFileId();
            state.removeFileHandle(fileId);
            log.info("CLOSE: File handle {} removed.", fileId);

            // Construct the CLOSE Response
            CloseResponse closeResponse = new CloseResponse();

            // If SMB2_CLOSE_FLAG_POSTQUERY_ATTRIB is set, return file attributes
            if ((closeRequest.getFlags() & Smb2Constants.SMB2_CLOSE_FLAG_POSTQUERY_ATTRIB) != 0) {
                Path filePath = Paths.get(fileHandle.getPath());
                if (Files.exists(filePath)) {
                    BasicFileAttributes attrs = Files.readAttributes(filePath, BasicFileAttributes.class);
                    long currentTime = Instant.now().toEpochMilli() * 10000 + Smb2Constants.FILETIME_EPOCH_DIFF;

                    closeResponse.setCreationTime(attrs.creationTime().toMillis() * 10000 + Smb2Constants.FILETIME_EPOCH_DIFF);
                    closeResponse.setLastAccessTime(attrs.lastAccessTime().toMillis() * 10000 + Smb2Constants.FILETIME_EPOCH_DIFF);
                    closeResponse.setLastWriteTime(attrs.lastModifiedTime().toMillis() * 10000 + Smb2Constants.FILETIME_EPOCH_DIFF);
                    closeResponse.setChangeTime(attrs.lastModifiedTime().toMillis() * 10000 + Smb2Constants.FILETIME_EPOCH_DIFF);
                    closeResponse.setAllocationSize(attrs.size());
                    closeResponse.setEndOfFile(attrs.size());

                    int fileAttributes = 0;
                    if (Files.isDirectory(filePath)) {
                        fileAttributes |= Smb2Constants.FILE_ATTRIBUTE_DIRECTORY;
                    }
                    if (!Files.isWritable(filePath)) {
                        fileAttributes |= Smb2Constants.FILE_ATTRIBUTE_READONLY;
                    }
                    // TODO: Add more attributes as needed
                    closeResponse.setFileAttributes(fileAttributes);
                } else {
                    log.warn("CLOSE: File not found on disk for handle {}: {}", fileId, filePath);
                    // File might have been deleted, return default values or error
                    closeResponse.setCreationTime(0);
                    closeResponse.setLastAccessTime(0);
                    closeResponse.setLastWriteTime(0);
                    closeResponse.setChangeTime(0);
                    closeResponse.setAllocationSize(0);
                    closeResponse.setEndOfFile(0);
                    closeResponse.setFileAttributes(0x10); // FILE_ATTRIBUTE_NORMAL
                }
            } else {
                log.info("CLOSE: Post-query attributes flag not set.");
                // If flag not set, return default/zero values
                closeResponse.setCreationTime(0);
                closeResponse.setLastAccessTime(0);
                closeResponse.setLastWriteTime(0);
                closeResponse.setChangeTime(0);
                closeResponse.setAllocationSize(0);
                closeResponse.setEndOfFile(0);
                closeResponse.setFileAttributes(0x00);
            }

            // Build the full SMB2 response including the header
            Smb2Header responseHeader = new Smb2Header();
            responseHeader.setCommand((short) Smb2Constants.SMB2_CLOSE);
            responseHeader.setStatus(Smb2Constants.STATUS_SUCCESS);
            responseHeader.setFlags(Smb2Constants.SMB2_FLAG_RESPONSE);
            responseHeader.setMessageId(requestHeader.getMessageId());
            responseHeader.setCreditRequestResponse((short) 1);
            responseHeader.setSessionId(state.getSessionId());
            responseHeader.setTreeId(state.getTreeConnectId());

            Buffer responseBuffer = responseHeader.encode();
            Buffer bodyBuffer = closeResponse.encode();

            // 4. 合并并添加 NBSS 长度头 (4字节，大端)
            Buffer fullPacket = Buffer.buffer();
            int totalBodySize = responseBuffer.length() + bodyBuffer.length();

            // fullPacket.appendByte((byte) 0x00);      // Type: Session Message
            // fullPacket.appendMedium(totalBodySize);   // 24位长度 (大端)
            fullPacket.appendBuffer(responseBuffer);
            fullPacket.appendBuffer(bodyBuffer);

            return fullPacket;

        } catch (Exception e) {
            log.error("Error handling SMB2 CLOSE command: ", e);
            return buildErrorResponse(requestHeader, Smb2Constants.STATUS_INTERNAL_ERROR);
        }
    }

    private Buffer buildErrorResponse(Smb2Header requestHeader, long statusCode) {
        Smb2Header errorHeader = new Smb2Header();
        errorHeader.setFlags(Smb2Constants.SMB2_FLAG_RESPONSE);
        errorHeader.setMessageId(requestHeader.getMessageId());
        errorHeader.setStatus(statusCode);
        errorHeader.setCommand(requestHeader.getCommand());
        errorHeader.setSessionId(requestHeader.getSessionId());
        errorHeader.setTreeId(requestHeader.getTreeId());
        return errorHeader.encode();
    }
}

