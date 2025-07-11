package com.mycompany.rocksdb.model.acl;


import com.mycompany.rocksdb.model.PostOpAttr;
import com.mycompany.rocksdb.model.SerializablePayload;
import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class GETACL3resfail implements SerializablePayload {
  private PostOpAttr objAttributes;

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
