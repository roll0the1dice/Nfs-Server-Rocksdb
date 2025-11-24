package com.mycompany.rocksdb.nfs4;

import com.mycompany.rocksdb.netserver.XdrDecodingStream;
import io.vertx.reactivex.core.net.NetSocket;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data // 假设使用了 Lombok，或者手动写 getter/setter
@Builder
public class NfsRequestContext {
    private final int xid;
    private final NetSocket socket;
    private final Nfsv4Server.Nfsv4ConnectionState state;
    private final XdrDecodingStream xdr;
    private int seqid;
    // 未来如果有新参数只需改这里，不用改方法签名
}
