package com.mycompany.rocksdb.nfs4.reply;

import com.mycompany.rocksdb.netserver.XdrUtils;
import com.mycompany.rocksdb.nfs4.POJO.ChannelAttributes;
import io.vertx.reactivex.core.buffer.Buffer;

public class CreateSessionReply {
    // 常量定义，消除魔术数字
    public static final int CSR4_PERSIST = 0x00000002;

    private final long sessionIdHigh;
    private final long sessionIdLow;
    private final int sequenceId;
    private final int flags;
    private final ChannelAttributes foreChannel; // 前向通道 (Server -> Client)
    private final ChannelAttributes backChannel; // 后向通道 (Client -> Server)

    public CreateSessionReply(long sessionIdHigh, long sessionIdLow, int sequenceId, int flags,
                              ChannelAttributes foreChannel, ChannelAttributes backChannel) {
        this.sessionIdHigh = sessionIdHigh;
        this.sessionIdLow = sessionIdLow;
        this.sequenceId = sequenceId;
        this.flags = flags;
        this.foreChannel = foreChannel;
        this.backChannel = backChannel;
    }

    public Buffer encode() {
        Buffer buffer = Buffer.buffer();
        // Session ID (16 bytes)
        XdrUtils.writeLong(buffer, sessionIdHigh);
        XdrUtils.writeLong(buffer, sessionIdLow);

        // Sequence ID
        XdrUtils.writeInt(buffer, sequenceId);

        // Flags
        XdrUtils.writeInt(buffer, flags);

        // Fore Channel Attributes
        foreChannel.encode(buffer);

        // Back Channel Attributes
        backChannel.encode(buffer);

        return buffer;
    }
}
