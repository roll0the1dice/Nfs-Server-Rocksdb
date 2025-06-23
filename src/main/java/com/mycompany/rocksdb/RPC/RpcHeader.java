package com.mycompany.rocksdb.RPC;


import io.vertx.core.buffer.Buffer;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class RpcHeader implements DeserializablePayload{
    private int xid;
    private int msgType;
    private int rpcVersion;
    private int programNumber;
    private int programVersion;
    private int procedureNumber;
    private int credentialsFlavor;
    private int credentialsBodyLength;
    private CredentialsInRPC credentials;
    private int verifierFlavor;
    private int verififierLength;

    @Override
    public int fromBuffer(Buffer buffer, int startOffset) {
        int offset = startOffset;

        // Basic RPC header (24 bytes)
        setXid(buffer.getInt(offset));
        offset += 4;
        setMsgType(buffer.getInt(offset));
        offset += 4;
        setRpcVersion(buffer.getInt(offset));
        offset += 4;
        setProgramNumber(buffer.getInt(offset));
        offset += 4;
        setProgramVersion(buffer.getInt(offset));
        offset += 4;
        setProcedureNumber(buffer.getInt(offset));
        offset += 4;

        // Credentials (at least 8 bytes)
        setCredentialsFlavor(buffer.getInt(offset));
        offset += 4;
        setCredentialsBodyLength(buffer.getInt(offset));
        offset += 4;

        if (getCredentialsBodyLength() > 0) {
            byte[] credBytes = buffer.slice(offset, offset + getCredentialsBodyLength()).getBytes();
            setCredentials(new CredentialsInRPC(credBytes));
        }

        // RPC/XDR 协议要求字段按4字节对齐，所以需要计算填充后的长度
        int paddedCredLength = (getCredentialsBodyLength() + 3) & ~3;
        offset += paddedCredLength; // Move offset past the padded credentials body

        // Verifier (at least 8 bytes)
        setVerifierFlavor(buffer.getInt(offset));
        offset += 4;
        setVerififierLength(buffer.getInt(offset));
        offset += 4; // The next field starts after the verifier length

        return offset - startOffset;
    }
}
