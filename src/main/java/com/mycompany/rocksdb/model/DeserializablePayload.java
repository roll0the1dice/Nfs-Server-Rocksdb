package com.mycompany.rocksdb.model;

import io.vertx.core.buffer.Buffer;

public interface DeserializablePayload {
  void deserialize(Buffer buffer, int startingOffset);
  void deserialize();
  int getDeserializedSize();
}
