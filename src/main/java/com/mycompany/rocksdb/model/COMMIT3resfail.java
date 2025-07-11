package com.mycompany.rocksdb.model;

import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class COMMIT3resfail implements SerializablePayload {
  private WccData fileWcc;

  @Override
  public void serialize(ByteBuffer buffer) {
    fileWcc.serialize(buffer);
  }

  @Override
  public int getSerializedSize() {
    return fileWcc.getSerializedSize();
  }

  @Override
  public void serialize(Buffer buffer) {
    fileWcc.serialize(buffer);
  }
}
