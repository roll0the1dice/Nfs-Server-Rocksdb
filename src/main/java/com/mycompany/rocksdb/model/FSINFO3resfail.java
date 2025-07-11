package com.mycompany.rocksdb.model;


import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class FSINFO3resfail implements SerializablePayload {
  private PostOpAttr objAttributes; // post_op_attr present flag

  @Override
  public void serialize(ByteBuffer buffer) {
    objAttributes.serialize(buffer);
  }

  @Override
  public int getSerializedSize() {
    return objAttributes.getSerializedSize();
  }

  @Override
  public void serialize(Buffer buffer) {
    objAttributes.serialize(buffer);
  }
}
