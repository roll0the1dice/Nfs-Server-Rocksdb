package com.mycompany.rocksdb.smb2;

import com.mycompany.rocksdb.smb2.POJO.Smb2Header;

import io.vertx.reactivex.core.buffer.Buffer;

public class Smb2FlushHandler implements Smb2OperationHandler {

    @Override
    public Buffer handle(Smb2RequestContext context, Smb2Header header, int currentReqOffset) {
        // --- 1. 更新 SMB2 Header ---
        
        // 设置状态码为 SUCCESS。
        // 注意：如果你的服务器找不到请求中指定的 FileId，这里应该返回 STATUS_FILE_CLOSED (0xC0000128)
        header.setStatus(0); 
        
        // 设置响应标志位 (Server to Redirection)
        header.setFlags(header.getFlags() | 0x00000001);
        
        // 返还信用额度
        header.setCreditRequestResponse((short)Math.max(header.getCreditRequestResponse(), 1));

        // --- 2. 构建 SMB2 FLUSH Response Body ---
        Buffer replyBody = Buffer.buffer();
        
        // StructureSize (2 bytes): 协议规定必须为 4
        replyBody.appendShortLE((short) 4);
        
        // Reserved (2 bytes): 必须为 0
        replyBody.appendShortLE((short) 0);
        
        return replyBody;
    }
    
}
