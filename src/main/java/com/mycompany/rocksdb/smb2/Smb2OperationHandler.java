package com.mycompany.rocksdb.smb2;

import com.mycompany.rocksdb.smb2.POJO.Smb2Header;

import io.vertx.reactivex.core.buffer.Buffer;

@FunctionalInterface
public interface Smb2OperationHandler {
    Buffer handle(Smb2RequestContext context, Smb2Header header, int currentReqOffset);
}

