package com.mycompany.rocksdb.smb2;

import com.mycompany.rocksdb.smb2.POJO.NegotiateRequest;
import com.mycompany.rocksdb.smb2.POJO.NegotiateResponse;
import com.mycompany.rocksdb.smb2.POJO.Smb2Header;

import io.vertx.reactivex.core.buffer.Buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class Smb2NegotiateHandler implements Smb2OperationHandler {
    private static final Logger log = LoggerFactory.getLogger(Smb2NegotiateHandler.class);

    // Supported dialects, ordered by preference (highest version first)
    private static final short[] SUPPORTED_DIALECTS = {
            // Smb2Constants.SMB3_DIALECT_0311,
            // Smb2Constants.SMB3_DIALECT_0302,
            Smb2Constants.SMB3_DIALECT_030,
            Smb2Constants.SMB2_DIALECT_021,
            Smb2Constants.SMB2_DIALECT_002
    };
    private static final Set<Short> SUPPORTED_DIALECT_SET = new HashSet<>();

    static {
        for (short dialect : SUPPORTED_DIALECTS) {
            SUPPORTED_DIALECT_SET.add(dialect);
        }
    }

    @Override
    public Buffer handle(Smb2RequestContext context, Smb2Header requestHeader, int currentReqOffset) {
        int bodyOffset = Smb2Header.STRUCTURE_SIZE + currentReqOffset; // SMB2 header is 64 bytes

        try {
            NegotiateRequest negotiateRequest = NegotiateRequest.decode(context.getSmb2Message(), bodyOffset);
            log.info("Client dialects: {}", negotiateRequest.getDialects());

            short negotiatedDialect = Smb2Constants.SMB2_DIALECT_002; // Default to SMB 2.0.2
            boolean dialectFound = false;

            // Find the highest supported common dialect
            for (short supported : SUPPORTED_DIALECTS) {
                if (negotiateRequest.getDialects().contains(supported)) {
                    negotiatedDialect = supported;
                    dialectFound = true;
                    break; // Found highest common dialect
                }
            }

            if (!dialectFound) {
                log.warn("No common SMB2 dialect found with client. Client dialects: {}", negotiateRequest.getDialects());
                // According to spec, if no common dialect, server SHOULD respond with STATUS_SMB_BAD_DIALECT
                return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_INVALID_PARAMETER);
            }

            log.info("Negotiated dialect: 0x{}", Integer.toHexString(negotiatedDialect));

            // 1. 更新连接状态 (Client GUID)
            // 假设 getClientGuid() 返回的是 byte[]
            Smb2ConnectionState state = context.getState();
            state.setClientGuid(negotiateRequest.getClientGuid()); 

            // 2. 构建 Body (使用 LE)
            Buffer bodyBuffer = Buffer.buffer();
            bodyBuffer.appendShortLE((short) 65); // StructureSize 0x41
            bodyBuffer.appendShortLE(Smb2Constants.SMB2_NEGOTIATE_SIGNING_ENABLED); // SecurityMode
            state.setSecurityMode((byte)Smb2Constants.SMB2_NEGOTIATE_SIGNING_ENABLED);
            bodyBuffer.appendShortLE((short) negotiatedDialect);  // DialectRevision
            state.setDialectRevision(negotiatedDialect);
            bodyBuffer.appendShortLE((short) 0); // NegotiateContextCount (除非是 3.1.1)

            // ServerGuid (16字节)
            long msb = Smb2Utils.generateServerGuidHigh();
            long lsb = Smb2Utils.generateServerGuidLow();
            bodyBuffer.appendLongLE(lsb);
            bodyBuffer.appendLongLE(msb);
            state.setServerGuidLow(lsb);
            state.setServerGuidHigh(msb);

            // Capabilities
            bodyBuffer.appendIntLE(Smb2Constants.SMB2_GLOBAL_CAP_LEASING);
            state.setCapabilities(Smb2Constants.SMB2_GLOBAL_CAP_LEASING);
            bodyBuffer.appendIntLE(Smb2Constants.SMB2_MAX_BUFFER_SIZE); // MaxTransactSize
            bodyBuffer.appendIntLE(Smb2Constants.SMB2_MAX_BUFFER_SIZE); // MaxReadSize
            bodyBuffer.appendIntLE(Smb2Constants.SMB2_MAX_BUFFER_SIZE); // MaxWriteSize

            // 系统时间
            long currentTime = Smb2Utils.getPreciseFileTime();
            bodyBuffer.appendLongLE(currentTime); 
            bodyBuffer.appendLongLE(currentTime); // ServerStartTime

            // 安全认证相关
            // SecurityBuffer 偏移和长度 (假设现在没有安全令牌)
            bodyBuffer.appendShortLE((short) 128); // Offset: 64(Header) + 64(Body结构) 安全缓冲区偏移量
            bodyBuffer.appendShortLE((short) 0);   // Length: 0 安全缓冲区长度
            bodyBuffer.appendIntLE(0);             // Reserved/NegotiateContextOffset  安全二进制大对象

            // 3. 构建 Header (64字节)
            Smb2Header responseHeader = new Smb2Header();

            // 【最关键】设置 Flags 为 0x01 (SERVER_TO_REDIR)，表示这是一个响应包
            responseHeader.setFlags(Smb2Constants.SMB2_FLAGS_SERVER_TO_REDIR); 

            // 设置状态码为成功 (0)
            responseHeader.setStatus(Smb2Constants.STATUS_SUCCESS);

            // 设置命令号，必须与请求一致 (Negotiate 是 0)
            responseHeader.setCommand(Smb2Constants.SMB2_COMMAND_NEGOTIATE);

            // 设置 MessageId，必须与客户端发来的请求中的 MessageId 完全一致
            responseHeader.setMessageId(requestHeader.getMessageId());

            // 给予信用额度 (建议 64 或以上)
            responseHeader.setCreditRequestResponse((short) 32);

            // 编码 Header
            Buffer headerBuffer = responseHeader.encode();

            // 4. 合并并添加 NBSS 长度头 (4字节，大端)
            Buffer fullPacket = Buffer.buffer();
            int totalBodySize = headerBuffer.length() + bodyBuffer.length();

            // fullPacket.appendByte((byte) 0x00);      // Type: Session Message
            // fullPacket.appendMedium(totalBodySize);   // 24位长度 (大端)
            fullPacket.appendBuffer(headerBuffer);
            fullPacket.appendBuffer(bodyBuffer);

            return fullPacket;

        } catch (Exception e) {
            log.error("Error handling SMB2 NEGOTIATE command: ", e);
            return Smb2Utils.buildErrorResponse(requestHeader, Smb2Constants.STATUS_INVALID_PARAMETER);
        }
    }
}

