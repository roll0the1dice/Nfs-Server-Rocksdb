package com.mycompany.rocksdb.smb2;

import com.mycompany.rocksdb.smb2.POJO.ReadRequest;
import com.mycompany.rocksdb.smb2.POJO.ReadResponse;
import com.mycompany.rocksdb.smb2.POJO.Smb2Header;
import io.vertx.reactivex.core.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.Future;

public class Smb2ReadHandler implements Smb2OperationHandler {
    private static final Logger log = LoggerFactory.getLogger(Smb2ReadHandler.class);
    private static final String SHARE_ROOT_PATH = "/tmp/smb2_share";

    @Override
    public Buffer handle(Smb2RequestContext context, Smb2Header requestHeader, int currentReqOffset) {
        int bodyOffset = Smb2Header.STRUCTURE_SIZE + currentReqOffset; // SMB2 header is 64 bytes

        try {
            ReadRequest readRequest = ReadRequest.decode(context.getSmb2Message(), bodyOffset);

            log.info("READ: FileId: {}/{}, Offset: {}, Length: {}",
                    readRequest.getFileIdVolatile(),
                    readRequest.getFileIdPersistent(),
                    readRequest.getOffset(),
                    readRequest.getLength());

            Smb2ConnectionState state = context.getState();
            if (state.getSessionId() == 0) {
                log.warn("READ: No active session for client.");
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_USER_SESSION_NOT_FOUND);
            }

            if (state.getTreeConnectId() == 0) {
                log.warn("READ: No active tree connect for client.");
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_NETWORK_NAME_DELETED);
            }

            long fileId = readRequest.getFileIdPersistent();
            Smb2ConnectionState.Smb2FileHandle fileHandle = state.getFileHandle(fileId);
            if (fileHandle == null) {
                log.warn("READ: File handle not found for fileId: {}", fileId);
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_FILE_CLOSED);
            }

            Path filePath = Paths.get(fileHandle.getPath());
            if (!Files.exists(filePath) || Files.isDirectory(filePath)) {
                log.warn("READ: File not found or is a directory: {}", filePath);
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_OBJECT_NAME_NOT_FOUND);
            }

            BasicFileAttributes attrs = Files.readAttributes(filePath, BasicFileAttributes.class);
            if (readRequest.getOffset() >= attrs.size()) {
                log.info("READ: No bytes read from file {}, offset {}, size {}", filePath, readRequest.getOffset(), attrs.size());
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_END_OF_FILE); 
            }

            // Read data from the file
            int bytesToRead = readRequest.getLength();
            long fileOffset = readRequest.getOffset();
            Buffer dataReadBuffer = Buffer.buffer();

            try (AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(filePath, StandardOpenOption.READ)) {
                ByteBuffer byteBuffer = ByteBuffer.allocate(bytesToRead);
                Future<Integer> operation = fileChannel.read(byteBuffer, fileOffset);
                Integer bytesRead = operation.get(); // Blocks until read is complete

                if (bytesRead > 0) {
                    byteBuffer.flip();
                    dataReadBuffer.appendBytes(byteBuffer.array(), 0, bytesRead);
                    log.info("READ: Read {} bytes from file {}", bytesRead, filePath);
                } else {
                    log.info("READ: No bytes read from file {}", filePath);
                    return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_END_OF_FILE); 
                }
            } catch (IOException e) {
                log.error("READ: Error reading from file {}: ", filePath, e);
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_ACCESS_DENIED);
            }

            // Construct the READ Response
            ReadResponse readResponse = new ReadResponse();
            readResponse.setStructureSize((short)ReadResponse.STRUCTURE_SIZE); 
            readResponse.setDataOffset((byte) (Smb2Header.STRUCTURE_SIZE + ReadResponse.responseHeaderSize)); // Data starts after SMB2 Header + Read Response Header
            readResponse.setDataLength(dataReadBuffer.length());
            readResponse.setDataRemaining(0); // For simplicity, assume all requested data is returned or no more data to read
            readResponse.setDataBuffer(dataReadBuffer);

            // Build the full SMB2 response including the header
            Smb2Header responseHeader = new Smb2Header();
            responseHeader.setCommand((short) Smb2Constants.SMB2_READ);
            responseHeader.setStatus(Smb2Constants.STATUS_SUCCESS);
            responseHeader.setFlags(Smb2Constants.SMB2_FLAG_RESPONSE);
            responseHeader.setMessageId(requestHeader.getMessageId());
            responseHeader.setCreditRequestResponse((short) 1);
            responseHeader.setSessionId(state.getSessionId());
            responseHeader.setTreeId(state.getTreeConnectId());

            Buffer responseBuffer = Buffer.buffer();
            responseBuffer.appendBuffer(responseHeader.encode());
            responseBuffer.appendBuffer(readResponse.encode());
            //responseBuffer.appendBuffer(readResponse.getDataBuffer()); // Append the actual data

            return responseBuffer;

        } catch (Exception e) {
            log.error("Error handling SMB2 READ command: ", e);
            return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_INTERNAL_ERROR);
        }
    }
}

