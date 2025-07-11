package com.mycompany.rocksdb.model;

import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SetMtime implements SerializablePayload, DeserializablePayload {
  public int setIt;
  public int mtimeSeconds;
  public int mtimeNSeconds;

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.putInt(setIt);
    if (setIt > 0) {
      buffer.putInt(mtimeSeconds);
      buffer.putInt(mtimeNSeconds);
    }
  }

  @Override
  public int getSerializedSize() {
    return 4 + (setIt > 0 ? 8 : 0);
  }

  @Override
  public void serialize(Buffer buffer) {
    buffer.appendInt(setIt);
    if (setIt > 0) {
      buffer.appendInt(mtimeSeconds);
      buffer.appendInt(mtimeNSeconds);
    }
  }

  @Override
  public void deserialize(Buffer buffer, int startingOffset) {
    int index = startingOffset;
    setIt = buffer.getInt(index);
  }

  @Override
  public void deserialize() {

  }

  @Override
  public int getDeserializedSize() {
    return 4;
  }
}
