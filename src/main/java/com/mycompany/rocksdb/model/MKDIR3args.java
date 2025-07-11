package com.mycompany.rocksdb.model;

import io.vertx.core.buffer.Buffer;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class MKDIR3args implements DeserializablePayload {
  public Diropargs3 where;
  public SetAttr3 attributes;

  @Override
  public void deserialize(Buffer buffer, int startingOffset) {
    int index = startingOffset;
    where = new Diropargs3();
    where.deserialize(buffer, index);
    index += where.getDeserializedSize();
    attributes = new SetAttr3();
    attributes.deserialize(buffer, index);
  }

  @Override
  public void deserialize() {

  }

  @Override
  public int getDeserializedSize() {
    return where.getDeserializedSize() + attributes.getDeserializedSize();
  }
}
