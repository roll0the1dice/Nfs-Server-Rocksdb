package com.mycompany.rocksdb.smb2;

import com.mycompany.rocksdb.smb2.POJO.Smb2Header;

import io.vertx.reactivex.core.buffer.Buffer;

public class Smb2EchoHandler implements Smb2OperationHandler {
    @Override
    public Buffer handle(Smb2RequestContext context, Smb2Header header, int currentReqOffset) {
        // --- 1. 修正 SMB2 Header (非常重要) ---
        
        // 设置状态码为 SUCCESS (0x00000000)
        header.setStatus(0); 
        
        // 设置 Flags: 必须包含 SERVER_TO_REDIRECTION (0x00000001)，表示这是一个响应包
        header.setFlags(header.getFlags() | 0x00000001);
        
        // 【核心修正】返还信用额度 (Credits)
        // 如果不返还 Credit，Linux 客户端在耗尽初始 Credit 后会直接卡死 (Hang)
        // 建议至少返回 1，通常返回原请求中的 CreditRequest 数量
        header.setCreditRequestResponse((short)Math.max(header.getCreditRequestResponse(), 1));

        // --- 2. 构建 SMB2 ECHO Response Body ---
        Buffer replyBody = Buffer.buffer();
        
        // StructureSize (2 bytes): 协议规定必须为 4
        replyBody.appendShortLE((short) 4);
        
        // Reserved (2 bytes): 必须为 0
        replyBody.appendShortLE((short) 0);
        
        // --- 3. 组合 Header 和 Body ---
        // 注意：取决于你的框架实现，有些框架会自动帮你拼 Header，
        // 如果需要手动拼，通常是 header.encode().appendBuffer(replyBody)
        return  header.encode().appendBuffer(replyBody); 
    }
}
