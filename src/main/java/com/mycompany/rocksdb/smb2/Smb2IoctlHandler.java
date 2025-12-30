package com.mycompany.rocksdb.smb2;

import com.mycompany.rocksdb.smb2.POJO.Smb2Header;
import com.mycompany.rocksdb.smb2.POJO.Smb2IoctlRequest;
import com.mycompany.rocksdb.smb2.POJO.Smb2IoctlResponse;
import io.vertx.reactivex.core.buffer.Buffer;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Smb2IoctlHandler implements Smb2OperationHandler {

    private static final Logger log = LoggerFactory.getLogger(Smb2IoctlHandler.class);

    @Override
    public Buffer handle(Smb2RequestContext context, Smb2Header requestHeader, int currentReqOffset) {
        Smb2ConnectionState state = context.getState();
        log.info("Handling SMB2 IOCTL command for MessageId: {}", requestHeader.getMessageId());

        // The IOCTL request payload starts after the SMB2 header (64 bytes)
        int ioctlRequestOffset = Smb2Header.SMB2_HEADER_SIZE + currentReqOffset;
        Smb2IoctlRequest ioctlRequest = Smb2IoctlRequest.decode(context.getSmb2Message(), ioctlRequestOffset);

        log.info("SMB2 IOCTL CtlCode: 0x{}", Integer.toHexString(ioctlRequest.getCtlCode()));

        // TODO: Implement actual IOCTL request parsing and handling logic here.
        // 判断是否是 FSCTL_VALIDATE_NEGOTIATE_INFO 请求
        ByteBuffer outputBuffer = ByteBuffer.allocate(0).order(ByteOrder.LITTLE_ENDIAN);
        byte[] outputData = new byte[0];  // 默认空输出，防止后续使用时为 null

        if (ioctlRequest.getCtlCode() == 0x00140204) {  // FSCTL_VALIDATE_NEGOTIATE_INFO
            outputBuffer = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);

            outputBuffer.putInt(state.getCapabilities());           // Capabilities（回显或服务器支持的值）
            outputBuffer.putLong(state.getServerGuidLow());         // ServerGuid Low part
            outputBuffer.putLong(state.getServerGuidHigh());        // ServerGuid High part
            outputBuffer.putShort((short) state.getSecurityMode()); // SecurityMode（注意可能需要强转 short）
            outputBuffer.putShort(state.getDialectRevision());      // DialectRevision，例如 0x0311

            outputData = outputBuffer.array();  // 32 字节响应

        } else if (ioctlRequest.getCtlCode() == 0x001401FC) {  // FSCTL_QUERY_NETWORK_INTERFACE_INFO
            // 固定部分 72 字节
            ByteBuffer iface = ByteBuffer.allocate(72).order(ByteOrder.LITTLE_ENDIAN);
            iface.putInt(1);                          // InterfaceIndex
            iface.putInt(0);                          // Reserved
            iface.putLong(104L);                      // Next offset = 72 + 32 = 104 字节（0x68），表示还有地址部分；如果这是最后一个，可设 0
            iface.putInt(0x0000000B);                 // Capabilities（常见值：SCALEOUT + CLUSTERED 等）
            iface.putInt(0);                          // Reserved2
            iface.putLong(10000000000L);              // LinkSpeed: 10 Gbits/s
            iface.putLong(0);                         // Reserved3

            // IPv4 地址部分（32 字节 SockAddr_In）
            ByteBuffer addr = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);
            addr.putShort((short) 2);                 // Family: AF_INET = 2
            addr.putShort((short) 0);                 // Port: 0
            addr.put((byte) 172);                     // IP: 172.17.0.1
            addr.put((byte) 17);
            addr.put((byte) 0);
            addr.put((byte) 1);
            // 剩余 24 字节 padding 填 0
            for (int i = 16; i < 32; i++) {
                addr.put((byte) 0);
            }

            // 组合成完整的 NETWORK_INTERFACE_INFO（72 + 32 = 104 字节）
            byte[] fullIface = new byte[104];
            System.arraycopy(iface.array(), 0, fullIface, 0, 72);
            System.arraycopy(addr.array(), 0, fullIface, 72, 32);

            outputData = fullIface;  // 赋值给统一的 outputData

        } else {
            // 其他未知或不支持的 IOCTL，返回空输出 + 合适的错误状态码（在外面设置 status）
            outputData = new byte[0];
        }
        // This will involve decoding the SMB2 IOCTL request structure,
        // processing the CtlCode, FileId, InputBuffer, etc.

        // For now, we'll create a generic success response.
        Smb2IoctlResponse ioctlResponse = new Smb2IoctlResponse();
        ioctlResponse.setStructureSize((short)Smb2IoctlResponse.STRUCTURE_SIZE);
        ioctlResponse.setCtlCode(ioctlRequest.getCtlCode());
        ioctlResponse.setFileIdVol(ioctlRequest.getFileIdVol());
        ioctlResponse.setFileIdEph(ioctlRequest.getFileIdEph());
        // Output buffer 设置
        // 计算偏移：Header 64 + Response 固定部分 49 = 113
        // 必须 8 字节对齐（113 % 8 = 1，所以需要 pad 7 字节）
        int fixedPartEnd = Smb2Header.SMB2_HEADER_SIZE + Smb2IoctlResponse.STRUCTURE_SIZE - 1;  // 113
        int padding = 0; // (8 - (fixedPartEnd % 8)) % 8;  // 7
        int outputOffset = fixedPartEnd + padding;   // 120
        ioctlResponse.setInputOffset(0); // No input buffer in response
        ioctlResponse.setInputCount(0);
  
        ioctlResponse.setOutputOffset(outputOffset); // Output buffer starts after SMB2 header + IOCTL response header
        ioctlResponse.setOutputCount(outputData.length); // No output data for generic success
        ioctlResponse.setOutputBuffer(outputData);
        ioctlResponse.setFlags(0);

        // Build the SMB2 header for the response
        Smb2Header responseHeader = new Smb2Header();
        responseHeader.setCommand(requestHeader.getCommand());
        responseHeader.setMessageId(requestHeader.getMessageId());
        responseHeader.setTreeId(requestHeader.getTreeId());
        responseHeader.setSessionId(requestHeader.getSessionId());
        responseHeader.setStatus(outputData.length > 0 ? Smb2Constants.STATUS_SUCCESS : Smb2Constants.STATUS_NOT_SUPPORTED);
        responseHeader.setFlags(Smb2Constants.SMB2_FLAG_RESPONSE);
        responseHeader.setCreditRequestResponse((short)1);
    
        // Encode the SMB2 header and append the IOCTL response
        Buffer headerBuffer = responseHeader.encode();
        Buffer bodyBuffer = ioctlResponse.encode();

        // 4. 合并并添加 NBSS 长度头 (4字节，大端)
        Buffer fullPacket = Buffer.buffer();
        int totalBodySize = headerBuffer.length() + bodyBuffer.length();

        // fullPacket.appendByte((byte) 0x00);      // Type: Session Message
        // fullPacket.appendMedium(totalBodySize);   // 24位长度 (大端)
        fullPacket.appendBuffer(headerBuffer);
        fullPacket.appendBuffer(bodyBuffer);

        return fullPacket;
    }
}
