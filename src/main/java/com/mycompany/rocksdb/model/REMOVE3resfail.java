package com.mycompany.rocksdb.model;

import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class REMOVE3resfail implements SerializablePayload {
  private WccData dirWcc;

  @Override
  public void serialize(ByteBuffer buffer) {
    dirWcc.serialize(buffer);
  }

  @Override
  public int getSerializedSize() {
    return dirWcc.getSerializedSize();
  }

  @Override
  public void serialize(Buffer buffer) {
    dirWcc.serialize(buffer);
  }
}
