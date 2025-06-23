package com.mycompany.rocksdb.DTO;


import com.mycompany.rocksdb.RPC.DeserializablePayload;
import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class MOUNT3args implements DeserializablePayload {
    private int pathLength; // object handle length
    private byte[] path; // object handle data

    @Override
    public int fromBuffer(Buffer buffer, int startOffset) {
        int offset = startOffset;

        setPathLength(buffer.getInt(offset));
        offset += 4;

        setPath(buffer.slice(offset, offset + pathLength).getBytes());
        offset += pathLength / 4 * 4 + 4;

        return offset - startOffset;
    }
}
