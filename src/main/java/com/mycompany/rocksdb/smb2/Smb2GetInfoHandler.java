package com.mycompany.rocksdb.smb2;

import com.mycompany.rocksdb.smb2.POJO.GetInfoRequest;
import com.mycompany.rocksdb.smb2.POJO.GetInfoResponse;
import com.mycompany.rocksdb.smb2.POJO.Smb2Header;
import io.vertx.reactivex.core.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;

public class Smb2GetInfoHandler implements Smb2OperationHandler {
    private static final Logger log = LoggerFactory.getLogger(Smb2GetInfoHandler.class);
    private static final String SHARE_ROOT_PATH = "/tmp/smb2_share";

    @Override
    public Buffer handle(Smb2RequestContext context, Smb2Header requestHeader, int currentReqOffset) {
        int bodyOffset = Smb2Header.STRUCTURE_SIZE + currentReqOffset; // SMB2 header is 64 bytes

        try {
            GetInfoRequest getInfoRequest = GetInfoRequest.decode(context.getSmb2Message(), bodyOffset);

            log.info("GET_INFO: InfoType: 0x{}, FileInfoClass: 0x{}, FileId: {}/{}",
                    Long.toHexString(getInfoRequest.getInfoType()),
                    Long.toHexString(getInfoRequest.getFileInfoClass()),
                    getInfoRequest.getFileIdVolatile(),
                    getInfoRequest.getFileIdPersistent());

            Smb2ConnectionState state = context.getState();
            if (state.getSessionId() == 0) {
                log.warn("GET_INFO: No active session for client.");
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_USER_SESSION_NOT_FOUND);
            }

            if (state.getTreeConnectId() == 0) {
                log.warn("GET_INFO: No active tree connect for client.");
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_NETWORK_NAME_DELETED);
            }

            long fileId = getInfoRequest.getFileIdPersistent();
            Smb2ConnectionState.Smb2FileHandle fileHandle = fileId == -1L ? state.getCurrentFileHandle() : state.getFileHandle(fileId);
            if (fileHandle == null) {
                log.warn("GET_INFO: File handle not found for fileId: {}", fileId);
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_FILE_CLOSED);
            }

            Path filePath = Paths.get(fileHandle.getPath());
            if (!Files.exists(filePath)) {  
                log.warn("GET_INFO: File not found on disk: {}", filePath);
                state.removeFileHandle(fileId); // Clean up stale handle
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_OBJECT_NAME_NOT_FOUND);
            }

            Buffer responseData = Buffer.buffer();
            long status = Smb2Constants.STATUS_SUCCESS;

            // Implement logic based on InfoType and FileInfoClass
            log.info("GET_INFO: InfoType: 0x{}", Long.toHexString(getInfoRequest.getInfoType()));
            switch (getInfoRequest.getInfoType()) {
                case Smb2Constants.SMB2_0_INFO_FILE:
                    responseData = handleFileInfoClass(filePath, getInfoRequest.getFileInfoClass(), requestHeader, state);
                    if (responseData == null) {
                        status = Smb2Constants.STATUS_INVALID_PARAMETER;
                    }
                    break;
              case Smb2Constants.SMB2_0_INFO_FILESYSTEM:
                    responseData = handleFileSystemInfoClass(filePath, (byte) getInfoRequest.getFileInfoClass());
                    if (responseData == null) {
                        status = Smb2Constants.STATUS_INVALID_PARAMETER;
                    }
                    break;
                // TODO: Add other InfoType cases (e.g., SMB2_0_INFO_FILESYSTEM, SMB2_0_INFO_SECURITY)
                default:
                    log.warn("Unsupported InfoType: 0x{}", Long.toHexString(getInfoRequest.getInfoType()));
                    status = Smb2Constants.STATUS_NOT_SUPPORTED;
                    break;
            }

            if (status != Smb2Constants.STATUS_SUCCESS || responseData == null) {
                return Smb2Utils.buildErrorResponse(requestHeader, status);
            }

            // Construct the GET_INFO Response
            GetInfoResponse getInfoResponse = new GetInfoResponse();
            getInfoResponse.setStructureSize((short) 9); // 1. StructureSize: 必须为 16
            getInfoResponse.setOutputBufferOffset((short) 72); // Data starts after SMB2 Header
            getInfoResponse.setOutputBufferLength(responseData.length()); // // 3. OutputBufferLength: 数据长度
            getInfoResponse.setDataPayload(responseData.getBytes());

            // Build the full SMB2 response including the header
            Smb2Header responseHeader = new Smb2Header();
            responseHeader.setCommand((short) requestHeader.getCommand());
            responseHeader.setStatus(Smb2Constants.STATUS_SUCCESS);
            responseHeader.setFlags(Smb2Constants.SMB2_FLAG_RESPONSE);
            responseHeader.setMessageId(requestHeader.getMessageId());
            responseHeader.setCreditRequestResponse((short) 1);
            responseHeader.setSessionId(state.getSessionId());
            responseHeader.setTreeId(state.getTreeConnectId());

            Buffer responseBuffer = responseHeader.encode();
            Buffer bodyBuffer = getInfoResponse.encode();

            // 4. 合并并添加 NBSS 长度头 (4字节，大端)
            Buffer fullPacket = Buffer.buffer();
            int totalBodySize = responseBuffer.length() + bodyBuffer.length();

            // fullPacket.appendByte((byte) 0x00);      // Type: Session Message
            // fullPacket.appendMedium(totalBodySize);   // 24位长度 (大端)
            fullPacket.appendBuffer(responseBuffer);
            fullPacket.appendBuffer(bodyBuffer);

            return fullPacket;

        } catch (Exception e) {
            log.error("Error handling SMB2 GET_INFO command: ", e);
            return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_INTERNAL_ERROR);
        }
    }

    private Buffer handleFileSystemInfoClass(Path filePath, byte fsInfoClass) {
        // 每一个请求都创建一个全新的 Buffer，防止数据污染
        Buffer responseData = Buffer.buffer();
        
        // 获取共享根目录路径（用于磁盘空间查询）
        // 假设你的根路径变量名为 SHARE_ROOT_PATH
        String rootPath = filePath.toAbsolutePath().toString();

        try {
            switch (fsInfoClass) {
                case Smb2Constants.FILE_FS_VOLUME_INFORMATION: // 0x01
                    // 1. Volume Creation Time (8 bytes)
                    responseData.appendLongLE(Smb2Utils.toFileTime(Files.readAttributes(filePath, BasicFileAttributes.class).creationTime()));
                    // 2. Volume Serial Number (4 bytes)
                    responseData.appendIntLE(0x12345678); 
                    // 3. Volume Label Length (4 bytes)
                    String volumeLabel = "SMB2_SHARE";
                    byte[] labelBytes = volumeLabel.getBytes(StandardCharsets.UTF_16LE);
                    responseData.appendIntLE(labelBytes.length);
                    // 4. Supports Objects (1 byte)
                    responseData.appendByte((byte) 0);
                    // 5. Reserved (1 byte) - MS-FSCC 2.5.9 规范要求是 1 字节
                    responseData.appendByte((byte) 0);
                    // 6. Volume Label (Variable)
                    responseData.appendBytes(labelBytes);
                    break;

                case Smb2Constants.FILE_FS_SIZE_INFORMATION: // 0x03 (有些 Linux 挂载会问这个)
                    FileStore storeBasic = Files.getFileStore(filePath);
                    long blockSizeBasic = 4096;
                    responseData.appendLongLE(storeBasic.getTotalSpace() / blockSizeBasic);     // TotalAllocationUnits
                    responseData.appendLongLE(storeBasic.getUsableSpace() / blockSizeBasic);    // AvailableAllocationUnits
                    responseData.appendIntLE(8);                                               // SectorsPerAllocationUnit
                    responseData.appendIntLE(512);                                             // BytesPerSector
                    break;

                case Smb2Constants.FILE_FS_DEVICE_INFORMATION: // 0x04
                    // 【注意】这里必须严格 8 字节！
                    // 1. DeviceType (4 bytes): FILE_DEVICE_DISK (0x00000007)
                    responseData.appendIntLE(0x00000007); 
                    // 2. Characteristics (4 bytes): FILE_DEVICE_IS_MOUNTED (0x00000020)
                    responseData.appendIntLE(0x00000020); 
                    break;

                case Smb2Constants.FILE_FS_ATTRIBUTE_INFORMATION: // 0x05
                    // 1. FileSystemAttributes (4 bytes): Case Sensitive, Unicode, etc.
                    responseData.appendIntLE(0x00000007);
                    // 2. MaximumComponentNameLength (4 bytes): 255
                    responseData.appendIntLE(255);
                    // 3. FileSystemNameLength (4 bytes)
                    String fsName = "NTFS"; 
                    byte[] fsNameBytes = fsName.getBytes(StandardCharsets.UTF_16LE);
                    responseData.appendIntLE(fsNameBytes.length);
                    // 4. FileSystemName (Variable)
                    responseData.appendBytes(fsNameBytes);
                    break;

                case Smb2Constants.FILE_FS_FULL_SIZE_INFORMATION: // 0x07 (32字节)
                    FileStore storeFull = Files.getFileStore(filePath);
                    long bytesPerUnit = 4096; // 假设簇大小
                    // 1. TotalAllocationUnits (8 bytes)
                    responseData.appendLongLE(storeFull.getTotalSpace() / bytesPerUnit);
                    // 2. CallerAvailableAllocationUnits (8 bytes)
                    responseData.appendLongLE(storeFull.getUsableSpace() / bytesPerUnit);
                    // 3. ActualAvailableAllocationUnits (8 bytes)
                    responseData.appendLongLE(storeFull.getUnallocatedSpace() / bytesPerUnit);
                    // 4. SectorsPerAllocationUnit (4 bytes)
                    responseData.appendIntLE(8); 
                    // 5. BytesPerSector (4 bytes)
                    responseData.appendIntLE(512); 
                    break;

                default:
                    log.warn("Unsupported FileSystem InfoClass: 0x{}", Integer.toHexString(fsInfoClass));
                    return null;
            }
        } catch (IOException e) {
            log.error("Error retrieving FileSystem information for class: 0x{}", Integer.toHexString(fsInfoClass), e);
            return null;
        }

        return responseData;
    }

    private Buffer handleFileInfoClass(Path filePath, byte fileInfoClass, Smb2Header requestHeader, Smb2ConnectionState state) throws Exception {
        BasicFileAttributes attrs = Files.readAttributes(filePath, BasicFileAttributes.class);
        Buffer dataBuffer = Buffer.buffer();

        // 根据不同的 Class 填充不同的结构
        switch (fileInfoClass) {
            case 0x01: // FILE_BASIC_INFORMATION (40 bytes)
                appendBasicInfo(dataBuffer, attrs, filePath);
                break;

            case 0x05: // FILE_STANDARD_INFORMATION (24 bytes)
                appendStandardInfo(dataBuffer, attrs, filePath);
                break;

            case 0x06: // FILE_INTERNAL_INFORMATION (8 bytes)
                appendInternalInfo(dataBuffer, requestHeader, state);
                break;

            case 0x12: // FILE_ALL_INFO (100 bytes if name length is 0)
                // ALL_INFO 是以上几个结构的复合排列
                appendBasicInfo(dataBuffer, attrs, filePath);    // 40 bytes
                appendStandardInfo(dataBuffer, attrs, filePath); // 24 bytes
                appendInternalInfo(dataBuffer, requestHeader, state); // 8 bytes
                
                dataBuffer.appendIntLE(0);          // EA Size (4 bytes)
                dataBuffer.appendIntLE(0x001F01FF); // Access Flags (4 bytes - Full Control)
                dataBuffer.appendLongLE(0);         // Position (8 bytes)
                dataBuffer.appendIntLE(0);          // Mode (4 bytes)
                dataBuffer.appendInt(1);          // Alignment (4 bytes)
                dataBuffer.appendIntLE(0);          // FileNameLength (4 bytes) - 挂载点为0
                dataBuffer.appendIntLE(0);          // FileNameLength (4 bytes) - 挂载点为0
                break;

            default:
                log.warn("Unsupported InfoClass: 0x{}", Integer.toHexString(fileInfoClass));
                return null;
        }
        return dataBuffer;
    }

    // --- 子结构填充方法 ---

    private void appendBasicInfo(Buffer buffer, BasicFileAttributes attrs, Path path) {
        // 必须正好 40 字节，且没有内部 StructureSize
        buffer.appendLongLE(Smb2Utils.toFileTime(attrs.creationTime()));
        buffer.appendLongLE(Smb2Utils.toFileTime(attrs.lastAccessTime()));
        buffer.appendLongLE(Smb2Utils.toFileTime(attrs.lastModifiedTime()));
        buffer.appendLongLE(Smb2Utils.toFileTime(attrs.lastModifiedTime())); // ChangeTime
        // buffer.appendLongLE(0L);    // CreationTime
        // buffer.appendLongLE(0L);    // LastAccessTime
        // buffer.appendLongLE(0L);    // LastWriteTime
        // buffer.appendLongLE(0L);    // ChangeTime
        
        int attr = 0;
        if (attrs.isDirectory()) attr |= 0x10; // FILE_ATTRIBUTE_DIRECTORY
        else attr |= 0x80; // FILE_ATTRIBUTE_NORMAL
        if (!Files.isWritable(path)) attr |= 0x01; // READONLY
        
        buffer.appendIntLE(attr);
        buffer.appendIntLE(0); // Reserved
    }

    private void appendStandardInfo(Buffer buffer, BasicFileAttributes attrs, Path path) {
        // 必须正好 24 字节
        buffer.appendLongLE(attrs.isDirectory() ? 0L : attrs.size()); // AllocationSize
        buffer.appendLongLE(attrs.isDirectory() ? 0L : attrs.size()); // EndOfFile
        buffer.appendIntLE(1); // NumberOfLinks
        buffer.appendByte((byte) 0); // DeletePending
        buffer.appendByte((byte) (attrs.isDirectory() ? 1 : 0)); // Directory
        buffer.appendShortLE((short) 0); // Reserved (Padding 使之达到24字节)
    }

    private void appendInternalInfo(Buffer buffer, Smb2Header header, Smb2ConnectionState state) {
        // 必须正好 8 字节 (IndexNumber/Inode)
        // 这里使用你为该文件分配的 FileId
        long fileId = state.getCurrentFileHandle().getFileId();
        buffer.appendLongLE(fileId); 
    }
}

