package com.mycompany.rocksdb.nfs4;

import io.vertx.reactivex.core.buffer.Buffer;

@FunctionalInterface
public interface NfsOperationHandler {
    Buffer handle(NfsRequestContext context);
}