package com.mycompany.rocksdb.smb2;

import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.net.NetSocket;

public class Smb2RequestContext {
    private final NetSocket socket;
    private final Smb2ConnectionState state;
    private final Buffer smb2Message;
    private long messageId; // From SMB2 header
    private int commandCode; // From SMB2 header

    public Smb2RequestContext(NetSocket socket, Smb2ConnectionState state, Buffer smb2Message) {
        this.socket = socket;
        this.state = state;
        this.smb2Message = smb2Message;
    }

    public NetSocket getSocket() {
        return socket;
    }

    public Smb2ConnectionState getState() {
        return state;
    }

    public Buffer getSmb2Message() {
        return smb2Message;
    }

    public long getMessageId() {
        return messageId;
    }

    public void setMessageId(long messageId) {
        this.messageId = messageId;
    }

    public int getCommandCode() {
        return commandCode;
    }

    public void setCommandCode(int commandCode) {
        this.commandCode = commandCode;
    }

    public byte[] getNtlmChallenge() {
        return null; // Placeholder for NTLM challenge retrieval
    }
}

