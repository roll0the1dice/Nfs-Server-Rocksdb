package com.mycompany.rocksdb.RPC;

import io.vertx.core.buffer.Buffer;

public interface DeserializablePayload {
    int fromBuffer(Buffer buffer, int startOffset);
}
