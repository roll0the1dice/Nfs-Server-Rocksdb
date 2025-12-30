package com.mycompany.rocksdb.smb2;

import com.mycompany.rocksdb.smb2.POJO.SessionSetupRequest;
import com.mycompany.rocksdb.smb2.POJO.SessionSetupResponse;
import com.mycompany.rocksdb.smb2.POJO.Smb2Header;

import io.vertx.reactivex.core.buffer.Buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class Smb2SessionSetupHandler implements Smb2OperationHandler {
    private static final Logger log = LoggerFactory.getLogger(Smb2SessionSetupHandler.class);
    private static final Random random = new Random();

    @Override
    public Buffer handle(Smb2RequestContext context, Smb2Header requestHeader, int currentReqOffset) {
        int bodyOffset = Smb2Header.STRUCTURE_SIZE + currentReqOffset; // SMB2 header is 64 bytes

        try {
            SessionSetupRequest sessionSetupRequest = SessionSetupRequest.decode(context.getSmb2Message(), bodyOffset);
            Smb2ConnectionState state = context.getState();

            // For now, a very rudimentary authentication check
            // In a real implementation, this would involve NTLM/Kerberos authentication
            boolean authenticated = true; // Assume success for now

            if (authenticated) {
                // 1. 准备安全认证数据 (Security Blob)
                // 如果是 NTLM 协商，这里通常应该是 NTLM Type 2 Challenge
                byte[] securityBlob = Smb2Utils.generateNtlmType2Message(); // 或者是空字节数组，视你的认证阶段而定
                if (securityBlob == null) securityBlob = new byte[0];

                // 2. 构造 SESSION_SETUP Response 结构
                SessionSetupResponse sessionSetupResponse = new SessionSetupResponse();
                // 必须：Response 的结构大小固定为 9
                sessionSetupResponse.setStructureSize((short) 9); 
                // 修正 Flag：通常初次登录成功或协商中不填 BINDING，除非是多通道绑定
                sessionSetupResponse.setSessionFlags((short) 0); 

                // 关键点：计算 SecurityBufferOffset
                // 偏移量 = Header 长度 (64) + SessionSetupResponse 固定部分长度 (8) = 72 (0x48)
                short blobOffset = 64 + 8; 
                sessionSetupResponse.setSecurityBufferOffset(blobOffset);
                sessionSetupResponse.setSecurityBufferLength((short) securityBlob.length);
                sessionSetupResponse.setSecurityBuffer(securityBlob);

                // 3. 构建 SMB2 Header
                Smb2Header responseHeader = new Smb2Header();
                responseHeader.setCommand((short) Smb2Constants.SMB2_SESSION_SETUP);
                // 如果认证还没完，状态应该是 STATUS_MORE_PROCESSING_REQUIRED (0xC0000016)
                // 如果已经认证通过，才是 STATUS_SUCCESS
                responseHeader.setStatus(Smb2Constants.STATUS_SUCCESS);
                responseHeader.setFlags(Smb2Constants.SMB2_FLAG_RESPONSE);
                responseHeader.setMessageId(requestHeader.getMessageId());
                responseHeader.setCreditRequestResponse((short) 1);
                long sessionId = requestHeader.getSessionId() == 0L ? Smb2Utils.generateNewSessionId() : requestHeader.getSessionId();
                responseHeader.setSessionId(sessionId);
                state.setSessionId(sessionId);

                // 4. 编码并组装
                Buffer responseHeaderBuffer = responseHeader.encode(); // 确保这个 encode() 输出 64 字节
                Buffer sessionSetupResponseBuffer = sessionSetupResponse.encode(); 
                // 注意：sessionSetupResponse.encode() 内部必须按顺序写入：
                // StructureSize(2), SessionFlags(2), Offset(2), Length(2), 最后才是 securityBlob

                int totalSmb2Size = responseHeaderBuffer.length() + sessionSetupResponseBuffer.length();

                Buffer fullPacket = Buffer.buffer();
                // NetBIOS 会话层报头
                // fullPacket.appendByte((byte) 0x00);        // Type: Session Message
                // fullPacket.appendMedium(totalSmb2Size);     // 24位长度 (大端)
                fullPacket.appendBuffer(responseHeaderBuffer);
                fullPacket.appendBuffer(sessionSetupResponseBuffer);

                return fullPacket;
            } else {
                log.warn("Authentication failed for SESSION_SETUP request.");
                return buildErrorResponse(requestHeader, Smb2Constants.STATUS_ACCESS_DENIED);
            }

        } catch (Exception e) {
            log.error("Error handling SMB2 SESSION_SETUP command: ", e);
            return buildErrorResponse(requestHeader, Smb2Constants.STATUS_INVALID_PARAMETER);
        }
    }

    private Buffer buildErrorResponse(Smb2Header requestHeader, long statusCode) {
        Smb2Header errorHeader = new Smb2Header();
        errorHeader.setFlags(Smb2Constants.SMB2_FLAG_RESPONSE);
        errorHeader.setMessageId(requestHeader.getMessageId());
        errorHeader.setStatus(statusCode);
        errorHeader.setCommand(requestHeader.getCommand());
        return errorHeader.encode();
    }
}

