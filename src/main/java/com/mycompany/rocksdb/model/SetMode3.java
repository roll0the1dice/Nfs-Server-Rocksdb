package com.mycompany.rocksdb.model;

import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SetMode3 implements DeserializablePayload{
  public int setIt;
  public int mode;

  @Override
  public void deserialize(Buffer buffer, int startingOffset) {
    int index = startingOffset;
    setIt = buffer.getInt(index);
    if (setIt == 1) {
      mode = buffer.getInt(index + 4);
    }
  }

  @Override
  public void deserialize() {

  }

  @Override
  public int getDeserializedSize() {
    return 4 + //
      (setIt == 1 ? 4 : 0);
  }
}
