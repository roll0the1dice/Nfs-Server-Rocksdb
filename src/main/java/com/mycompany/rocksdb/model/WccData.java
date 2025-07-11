package com.mycompany.rocksdb.model;

import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class WccData implements SerializablePayload {
  private PreOpAttr before;
  private PostOpAttr after;

  @Override
  public void serialize(ByteBuffer buffer) {
    before.serialize(buffer);
    after.serialize(buffer);
  }

  @Override
  public int getSerializedSize() {
    return before.getSerializedSize() + after.getSerializedSize();
  }

  @Override
  public void serialize(Buffer buffer) {
    before.serialize(buffer);
    after.serialize(buffer);
  }
}
