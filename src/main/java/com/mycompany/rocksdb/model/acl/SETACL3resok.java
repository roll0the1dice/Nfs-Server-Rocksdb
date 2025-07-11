package com.mycompany.rocksdb.model.acl;


import com.mycompany.rocksdb.model.PostOpAttr;
import com.mycompany.rocksdb.model.SerializablePayload;
import io.vertx.core.buffer.Buffer;

import java.nio.ByteBuffer;

public class SETACL3resok implements SerializablePayload {
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
