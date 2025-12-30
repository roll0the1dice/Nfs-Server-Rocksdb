package com.mycompany.rocksdb.smb2;

import com.mycompany.rocksdb.smb2.POJO.Smb2Header;
import com.mycompany.rocksdb.smb2.POJO.TreeConnectRequest;
import com.mycompany.rocksdb.smb2.POJO.TreeConnectResponse;

import io.vertx.reactivex.core.buffer.Buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class Smb2TreeConnectHandler implements Smb2OperationHandler {
    private static final Logger log = LoggerFactory.getLogger(Smb2TreeConnectHandler.class);
    private static final Random random = new Random();

    // For simplicity, hardcode a share for now
    private static final String SHARE_NAME = "\\\\127.0.0.1\\share";
    private static final String SHARE_PATH_ROOT = "/tmp/smb2_share"; // Local path for the share

    @Override
    public Buffer handle(Smb2RequestContext context, Smb2Header requestHeader, int currentReqOffset) {
        int bodyOffset = Smb2Header.STRUCTURE_SIZE + currentReqOffset; // SMB2 header is 64 bytes

        try {
            TreeConnectRequest treeConnectRequest = TreeConnectRequest.decode(context.getSmb2Message(), bodyOffset);
            log.info("Client requested share path: {}", treeConnectRequest.getPath());

            Smb2ConnectionState state = context.getState();
            if (state.getSessionId() == 0) {
                log.warn("TREE_CONNECT: No active session for client.");
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_USER_SESSION_NOT_FOUND); // Use appropriate error
            }

            long newTreeId = random.nextLong(); // Generate a new tree connect ID
            state.setTreeConnectId(newTreeId);
            log.info("Connected to share {}. New Tree ID: {}", treeConnectRequest.getPath(), newTreeId);

            // Construct the TREE_CONNECT Response
            TreeConnectResponse treeConnectResponse = new TreeConnectResponse();

            // 必须固定为 16 (0x0010)
            treeConnectResponse.setStructureSize((short) 16);

            // 共享类型：常见值
            // 0x10 = SMB2_SHARE_TYPE_DISK (磁盘共享，最常见)
            // 0x20 = SMB2_SHARE_TYPE_PIPE (命名管道)
            // 0x30 = SMB2_SHARE_TYPE_PRINT (打印队列)
            treeConnectResponse.setShareType((short) 0x01);  // 根据你的共享类型调整

            // Reserved 必须为 0
            treeConnectResponse.setReserved((byte) 0);

            // Share Flags (4 bytes, int 类型)
            // 常见组合示例：自动缓存 + 其他标志
            // SMB2_SHAREFLAG_AUTO_CACHING = 0x00000010
            // SMB2_SHAREFLAG_MANUAL_CACHING = 0x00000000
            // 其他如 SMB2_SHAREFLAG_ENCRYPT_DATA (SMB3+) 等
            treeConnectResponse.setShareFlags((short)0x00000000);  // 示例：启用自动缓存

            // Capabilities (4 bytes, int 类型)
            // 常见：0x00000000 (无特殊能力)
            // 如果是 DFS 共享：SMB2_SHARE_CAP_DFS = 0x00000008
            treeConnectResponse.setCapabilities(0x00000000);

            // Maximal Access (4 bytes, int 类型)
            // 常见完整权限：0x001F01FF (FILE_READ_DATA | FILE_WRITE_DATA | FILE_APPEND_DATA | FILE_EXECUTE | DELETE | READ/WRITE_ATTRIBUTES 等)
            // 或者更完整的 0x0012019F (常见于管理员访问)
            treeConnectResponse.setMaximalAccess(0x001F01FF);  // 根据实际用户/共享权限计算

            // Build the full SMB2 response including the header
            Smb2Header responseHeader = new Smb2Header();
            responseHeader.setCommand((short)Smb2Constants.SMB2_TREE_CONNECT);
            responseHeader.setStatus(Smb2Constants.STATUS_SUCCESS);
            responseHeader.setFlags(Smb2Constants.SMB2_FLAG_RESPONSE);
            responseHeader.setMessageId(requestHeader.getMessageId());
            responseHeader.setCreditRequestResponse((short) 1);
            responseHeader.setSessionId(state.getSessionId());
            responseHeader.setTreeId(newTreeId); // Set the new tree ID in the response header

            // 编码 Header
            Buffer headerBuffer = responseHeader.encode();
            Buffer bodyBuffer = treeConnectResponse.encode();

            // 4. 合并并添加 NBSS 长度头 (4字节，大端)
            Buffer fullPacket = Buffer.buffer();
            int totalBodySize = headerBuffer.length() + bodyBuffer.length();

            // fullPacket.appendByte((byte) 0x00);      // Type: Session Message
            // fullPacket.appendMedium(totalBodySize);   // 24位长度 (大端)
            fullPacket.appendBuffer(headerBuffer);
            fullPacket.appendBuffer(bodyBuffer);

            return fullPacket;

        } catch (Exception e) {
            log.error("Error handling SMB2 TREE_CONNECT command: ", e);
            return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_INVALID_PARAMETER);
        }
    }
}

