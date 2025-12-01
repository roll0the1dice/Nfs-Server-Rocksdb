package com.mycompany.rocksdb.nfs4;

import com.mycompany.rocksdb.netserver.*;
import com.mycompany.rocksdb.nfs4.POJO.ChannelAttributes;
import com.mycompany.rocksdb.nfs4.POJO.NfsFileAttributes;
import com.mycompany.rocksdb.nfs4.reply.CreateSessionReply;
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.disposables.Disposable;
import io.vertx.core.Future;
import io.vertx.core.net.NetServerOptions;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.net.NetServer;
import io.vertx.reactivex.core.net.NetSocket;
import io.vertx.reactivex.core.parsetools.RecordParser;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mycompany.rocksdb.myrocksdb.MyRocksDB;
import com.mycompany.rocksdb.utils.MetaKeyUtils;
import com.mycompany.rocksdb.POJO.Inode; // Import Inode
import com.mycompany.rocksdb.POJO.LatestIndexMetadata; // Import LatestIndexMetadata
import com.mycompany.rocksdb.POJO.VersionIndexMetadata;
import com.mycompany.rocksdb.POJO.Inode.Mode; // Import Inode.Mode
import com.mycompany.rocksdb.nfs4.Nfs4Access; // Import Nfs4Access
import com.mycompany.rocksdb.nfs4.Nfs4Attrs;
import com.mycompany.rocksdb.nfs4.Nfs4UnixMode; // Import Nfs4UnixMode
import com.mycompany.rocksdb.nfs4.Nfs4OpenConstants; // Import Nfs4OpenConstants
import com.mycompany.rocksdb.nfs4.Nfs4StateId; // Import Nfs4StateId
import com.mycompany.rocksdb.POJO.FileMetadata; // Import FileMetadata

import java.util.EnumMap;
import java.util.Map;
import java.util.Optional; // Import Optional
import java.util.Comparator; // Import Comparator
import java.util.stream.Collectors; // Import Collectors

import javax.management.RuntimeErrorException;

import java.nio.file.Paths; // Import Paths
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import io.vertx.reactivex.core.buffer.Buffer; // Import Vert.x Buffer
import java.util.List; // Import List
import java.nio.file.Path; // Import Path
import java.nio.file.Paths; // Import Paths
import java.util.Comparator; // Import Comparator
import java.util.stream.Collectors; // Import Collectors
import java.util.concurrent.ConcurrentHashMap; // Import ConcurrentHashMap


public class Nfsv4Server extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(Nfsv4Server.class);
    private static String HOST = "0.0.0.0";
    private static final int PORT = 2049; // Standard NFSv4 port

    private final CompositeDisposable disposables = new CompositeDisposable();
    private final Map<Nfs4Opcode, NfsOperationHandler> handlerMap = new EnumMap<>(Nfs4Opcode.class);
    private static final Inode ROOT_INODE = Inode.builder().build();

    // 初始化注册
    public void initHandlers() {
//        handlerMap.put(Nfs4Opcode.NFS4_EXCHANGE_ID, this::handleExchangeIdOperation);
//        handlerMap.put(Nfs4Opcode.NFS4_CREATE_SESSION, this::handleCreateSession);
//        handlerMap.put(Nfs4Opcode.NFS4_SEQUENCE, this::handleSequence);
        // 添加更多...

        ROOT_INODE.setNodeId(NfsConstants.ROOT_INODE_ID);
        int permission = 0x755;
        ROOT_INODE.setMode(Inode.Mode.S_IFDIR.getCode() | permission);  // // 040 表示目录，755 是权限

        // 设置所有者 (通常映射为 root:root)
        ROOT_INODE.setUid(0);
        ROOT_INODE.setGid(0);

            // 设置时间
        long now = System.currentTimeMillis();
        ROOT_INODE.setCtime(now);
        ROOT_INODE.setMtime(now);
        ROOT_INODE.setAtime(now);

        // 设置大小 (目录的大小通常是 block size 或者是 entry 数量，这里设为 4096)
        ROOT_INODE.setSize(4096);

        ROOT_INODE.setLinkN(2);

        ROOT_INODE.setObjName("/");

        log.info("Root inode {} created successfully.", ROOT_INODE);

    }

    @Override
    public void start(Future<Void> startPromise) throws Exception {
        NetServerOptions serverOptions = new NetServerOptions()
                .setReuseAddress(true)
                .setReusePort(true)
                .setHost(HOST)
                .setPort(PORT);

        NetServer server = vertx.createNetServer(serverOptions);

        Disposable serverSubscription = server.connectStream()
                .toFlowable()
                .subscribe(
                        this::handleNewConnection,
                        error -> log.error("NFSv4 Server connection error", error)
                );

        disposables.add(serverSubscription);

        server.rxListen(PORT)
                .subscribe(
                        s -> {
                            log.info("NFSv4 Server started on host " + HOST + " and port " + s.actualPort());
                            startPromise.complete();
                        },
                        error -> {
                            log.error("Failed to start NFSv4 server", error);
                            startPromise.fail(error);
                        }
                );

        initHandlers();
    }

    private void handleNewConnection(NetSocket socket) {
        log.info("NFSv4 Client connected: " + socket.remoteAddress());

        final Nfsv4ConnectionState connectionState = new Nfsv4ConnectionState();

        // NFSv4 命令处理将涉及 XDR 解码，这里只是一个占位符
        // 实际的 NFSv4 RPC 消息处理会复杂得多
        Flowable<Buffer> commandStream = socket.toFlowable()
                .compose(new NFSV4CommandFramer()); // 占位符

        Disposable socketSubscription = commandStream.subscribe(
                // onNext: 接收到一条完整的 NFSv4 RPC 消息 (XDR 编码)
                rpcMessage -> handleNfsv4Command(rpcMessage, socket, connectionState),
                // onError: 连接出错
                error -> {
                    log.error("Error on NFSv4 connection [" + socket.remoteAddress() + "]: ", error);
                    socket.close();
                },
                // onComplete: 客户端断开
                () -> log.info("NFSv4 Client disconnected: " + socket.remoteAddress())
        );

        disposables.add(socketSubscription);
    }

    private void handleNfsv4Command(Buffer rpcMessage, NetSocket socket, Nfsv4ConnectionState state) {
        // log.info("Received NFSv4 RPC message ({} bytes). This is a placeholder for actual XDR decoding and command handling.", rpcMessage.length());

        XdrDecodingStream xdr = new XdrDecodingStream(rpcMessage.getDelegate().getBytes());
        try {
            int xid = xdr.readInt();
            int msgType = xdr.readInt(); // CALL (0)

            if (msgType != RpcConstants.CALL) {
                log.error("Received non-CALL message type: {}", msgType);
                // TODO: Send appropriate RPC error response
                return;
            }

            int rpcVersion = xdr.readInt(); // RPC_VERSION (2)
            int program = xdr.readInt();    // NFS4_PROGRAM (100003)
            int version = xdr.readInt();    // NFS_V4 (4)
            int procedure = xdr.readInt(); // NFSPROC4_NULL (0), NFSPROC4_COMPOUND (1), etc.

            // Authentication info (should be AUTH_NONE for now)
            int credentialFlavor = xdr.readInt(); // credential flavor
            int credentialLength = xdr.readInt(); // credential length
            Buffer credential = xdr.readFixedOpaque(credentialLength); // Read credential data
            int verifierFlavor = xdr.readInt(); // verifier flavor
            int verifierLength = xdr.readInt(); // verifier length
            Buffer verifier = xdr.readFixedOpaque(verifierLength); // Read verifier data

            log.info("NFSv4 RPC Call: xid={}, program={}, version={}, procedure={}", Integer.toUnsignedString(xid), program, version, procedure);

            if (credentialLength > 0) {
                log.info("Credential data: {}", credential.toString());
            }
            if (verifierLength > 0) {
                log.info("Verifier data: {}", verifier.toString());
            }

            NfsRequestContext nfsRequestContext = NfsRequestContext.builder().socket(socket).state(state).xid(xid).xdr(xdr).build();

            switch (procedure) {
                case RpcConstants.NFSPROC4_NULL:
                    handleNullOperation(nfsRequestContext);
                    break;
                case RpcConstants.NFSPROC4_COMPOUND:
                    // TODO: Implement COMPOUND operation handling
                    //log.warn("NFSv4 COMPOUND operation not yet implemented.");
                    //sendRpcError(xid, socket, RpcConstants.PROG_UNAVAIL);
                    handleCompoundOperation(nfsRequestContext);
                    break;
                default:
                    log.warn("Unknown NFSv4 procedure: {}", procedure);
                    sendRpcError(xid, socket, RpcConstants.PROC_UNAVAIL);
                    break;
            }
        } catch (Exception e) {
            log.error("Error decoding NFSv4 RPC message: ", e);
            // TODO: Send a GARBAGE_ARGS or SYSTEM_ERR reply
        }
    }

    private void handleNullOperation(NfsRequestContext nfsRequestContext) {
        log.info("Handling NFSv4 NULL operation for xid: {}", Integer.toUnsignedString(nfsRequestContext.getXid()));
        // A NULL operation typically just returns a successful RPC reply with no payload.
        Buffer response = buildRpcReply(nfsRequestContext.getXid(), RpcConstants.MSG_ACCEPTED, RpcConstants.SUCCESS);
        log.info("response: {}", response.length());
        nfsRequestContext.getSocket().write(response);
    }

    public static byte[] binaryStringToByteArray(String binaryString) {
        // 检查字符串长度是否为 8 的倍数
        if (binaryString.length() % 8 != 0) {
            throw new IllegalArgumentException("二进制字符串的长度必须是 8 的倍数");
        }

        int numBytes = binaryString.length() / 8;
        byte[] byteArray = new byte[numBytes];

        for (int i = 0; i < numBytes; i++) {
            // 每次截取 8 位（一个字节）的子字符串
            String byteSubstring = binaryString.substring(i * 8, (i + 1) * 8);

            // 将二进制子字符串解析为整数（范围 0-255）
            int intValue = Integer.parseInt(byteSubstring, 2);

            // 将整数强制转换为 byte（范围 -128 到 127）
            // 注意：如果 intValue > 127，转换后会变成负数，但其底层的位模式是正确的
            byteArray[i] = (byte) intValue;
        }

        return byteArray;
    }
    
    private void handleCompoundOperation(NfsRequestContext nfsRequestContext) {
        XdrDecodingStream xdr = nfsRequestContext.getXdr();
        int xid = nfsRequestContext.getXid();
        log.info("Handling NFSv4 Compound operation for xid: {}", Integer.toUnsignedString(xid));
        // A NULL operation typically just returns a successful RPC reply with no payload.
        int tagLength = xdr.readInt(); // tag length
        Buffer tag = xdr.readFixedOpaque(tagLength); // Read tag data
        int minorVersion = xdr.readInt(); // minor version
        int opCount = xdr.readInt(); // number of operations
        log.info("Compound - Tag: {}, Minor Version: {}, Operation Count: {}", tag.toString(StandardCharsets.UTF_8), minorVersion, opCount);
        // For now, just send a generic success reply

        Buffer rpcHeaderBuffer = getReplyHeader(xid);

        // EXCHANGE_ID specific response payload
        Buffer rpcBodyBuffer = Buffer.buffer();

        // EID4_SERVER_OWNER
        XdrUtils.writeInt(rpcBodyBuffer, 0); // Status OK for operation
        XdrUtils.writeOpaque(rpcBodyBuffer, null); // Placeholder for tag
        XdrUtils.writeInt(rpcBodyBuffer, opCount); // number of operations

        for (int i = 0; i < opCount; i++) {
            Nfs4Opcode opCode = Nfs4Opcode.fromValue(xdr.readInt()); // Read first operation code
            int seqid = i+1;

            XdrUtils.writeInt(rpcBodyBuffer, opCode.getValue()); // Write operation code back
            XdrUtils.writeInt(rpcBodyBuffer, 0); // Placeholder for operation status OK

            nfsRequestContext.setSeqid(seqid);

            dispatchOperation(nfsRequestContext, opCode, rpcBodyBuffer);
        }
        //Buffer response = buildRpcReply(xid, RpcConstants.MSG_ACCEPTED, RpcConstants.SUCCESS);

        final int rpcMessageBodyLength = rpcBodyBuffer.length() + rpcHeaderBuffer.length();
        int recordMarkValue = 0x80000000 | rpcMessageBodyLength;

        Buffer fullResponseBuffer = Buffer.buffer();
        fullResponseBuffer.appendInt(recordMarkValue);
        fullResponseBuffer.appendBuffer(rpcHeaderBuffer);
        fullResponseBuffer.appendBuffer(rpcBodyBuffer);

        nfsRequestContext.getSocket().write(fullResponseBuffer);
    }

    private void dispatchOperation(NfsRequestContext nfsRequestContext, Nfs4Opcode opCode, Buffer rpcBodyBuffer) {
        int xid = nfsRequestContext.getXid();
        switch (opCode) {
            case NFS4_EXCHANGE_ID:
                 rpcBodyBuffer.appendBuffer(handleExchangeIdOperation(nfsRequestContext));
                 break;
            case NFS4_CREATE_SESSION:
                log.info("Handling NFSv4 CREATE_SESSION operation for xid: {}", Integer.toUnsignedString(xid));
                rpcBodyBuffer.appendBuffer(handleCreateSession(nfsRequestContext));
                break;
            case NFS4_SEQUENCE:
                rpcBodyBuffer.appendBuffer(handleSequence(nfsRequestContext));
                break;
            case NFS4_SECINFO_NO_NAME:
                log.info("Handling NFSv4 SECINFO_NO_NAME operation for xid: {}", Integer.toUnsignedString(xid));
                // For now, just return success with no additional data
                XdrUtils.writeInt(rpcBodyBuffer, 1);
                XdrUtils.writeInt(rpcBodyBuffer, 1);
                break;
            case NFS4_GETFH:
                long inodeId = nfsRequestContext.getState().getCurrentInodeId();
                byte[] bytesBig = ByteBuffer.allocate(8).putLong(inodeId).array();
                XdrUtils.writeOpaque(rpcBodyBuffer, bytesBig);
                break;
            case NFS4_GETATTR:
                log.info("Handling NFSv4 GETATTR operation for xid: {}", Integer.toUnsignedString(xid));
                rpcBodyBuffer.appendBuffer(handleNFSGetAttr(nfsRequestContext));
                break;
            case NFS4_ACCESS:
                log.info("Handling NFSv4 ACCESS operation for xid: {}", Integer.toUnsignedString(xid));
                rpcBodyBuffer.appendBuffer(handleNfs4Access(nfsRequestContext));
                break;
            case NFS4_PUTFH:
                 rpcBodyBuffer.appendBuffer(handlePutfhOperation(nfsRequestContext));
                 break;
            case NFS4_PUTROOTFH:
                 rpcBodyBuffer.appendBuffer(handlePutrootfhOperation(nfsRequestContext));
                 break;
            case NFS4_PUTPUBFH:
                // For simplicity, treat PUTPUBFH similarly to PUTROOTFH for now.
                rpcBodyBuffer.appendBuffer(handlePutrootfhOperation(nfsRequestContext));
                break;
            case NFS4_READDIR:
                log.info("Handling NFSv4 READDIR operation for xid: {}", Integer.toUnsignedString(xid));
                rpcBodyBuffer.appendBuffer(handleNfs4Readdir(nfsRequestContext));
                break;
            case NFS4_READ:
                log.info("Handling NFSv4 READ operation for xid: {}", Integer.toUnsignedString(xid));
                rpcBodyBuffer.appendBuffer(handleNfs4Read(nfsRequestContext));
                break;
            case NFS4_OPEN:
                log.info("Handling NFSv4 OPEN operation for xid: {}", Integer.toUnsignedString(xid));
                rpcBodyBuffer.appendBuffer(handleNfs4Open(nfsRequestContext));
                break;
            default:
        }
    }

    private Buffer handleNFSGetAttr(NfsRequestContext nfsRequestContext) {
        XdrDecodingStream xdr = nfsRequestContext.getXdr();
        int xid = nfsRequestContext.getXid();
        Nfsv4Server.Nfsv4ConnectionState state = nfsRequestContext.getState();
        // 1. 解析请求参数 (Bitmap)
        int attrBitmapLen = xdr.readInt();
        int[] reqBitmaps = new int[attrBitmapLen];
        for (int i = 0; i < attrBitmapLen; i++) {
            reqBitmaps[i] = xdr.readInt();
            //if (i == 2) reqBitmaps[i] = 0;
            log.debug("GETATTR xid: {}, mask: {}, mask[{}]: 0x{}",
                    Integer.toHexString(xid), Integer.toHexString(reqBitmaps[i]), i, Integer.toHexString(reqBitmaps[i]));
        }

        Buffer rpcBodyBuffer = Buffer.buffer();

        // 2. 获取业务数据 (Inode)
        long currentInodeId = state.getCurrentInodeId();
        if (currentInodeId == 0) {
            XdrUtils.writeInt(rpcBodyBuffer, Nfs4Errors.NFS4ERR_NOFILEHANDLE);
            return rpcBodyBuffer;
        }

        Optional<Inode> inodeOpt = currentInodeId != NfsConstants.ROOT_INODE_ID ? MyRocksDB.getINodeMetaData(state.targetVnodeId, state.bucket, currentInodeId) : Optional.of(ROOT_INODE);
        if (!inodeOpt.isPresent()) {
            log.warn("GETATTR: Inode not found for ID: {}", currentInodeId);
            XdrUtils.writeInt(rpcBodyBuffer, Nfs4Errors.NFS4ERR_BADHANDLE);
            return rpcBodyBuffer;
        }

        // 3. 构建并发送响应
        // 写入操作状态成功
        //XdrUtils.writeInt(rpcBodyBuffer, Nfs4Errors.NFS4_OK);

        // 使用封装好的类来处理属性编码
        Buffer responseBuffer = NfsFileAttributes.build(inodeOpt.get(), reqBitmaps, nfsRequestContext);

        // byte[] debugBytes = responseBuffer.getBytes();
        // StringBuilder hex = new StringBuilder();
        // for (byte b : debugBytes) {
        //     hex.append(String.format("%02X ", b));
        // }
        // log.info("NFS Response Hex: {}", hex.toString());

        return rpcBodyBuffer.appendBuffer(responseBuffer);
    }

    private Buffer handleExchangeIdOperation(NfsRequestContext nfsRequestContext) {
        XdrDecodingStream xdr = nfsRequestContext.getXdr();
        int xid = nfsRequestContext.getXid();
        int seqid = nfsRequestContext.getSeqid();

        log.info("Handling NFSv4 EXCHANGE_ID operation for xid: {}", Integer.toUnsignedString(xid));
        try {
            // Decode EXCHANGE_ID arguments (simplified for now)
            long verifier = xdr.readLong(); // Skip EXCHANGE_ID arguments for now
            int dataLength = xdr.readInt();
            Buffer data = xdr.readFixedOpaque(dataLength);
            log.info("Compound - EXCHANGE_ID operation with verifier: 0x{}, data length: {}", Long.toHexString(verifier), dataLength);
            int flags = xdr.readInt(); // Skip flags
            int eia_state_protect = xdr.readInt();
            int eia_client_impl_id = xdr.readInt();
            if (eia_client_impl_id > 0) {
                int DNSDomainLength = xdr.readInt();
                Buffer DNSDomain = xdr.readFixedOpaque(DNSDomainLength);
                log.info("Compound - EXCHANGE_ID DNS Domain: {}", DNSDomain.toString(StandardCharsets.UTF_8)); 
                int productName = xdr.readInt();
                Buffer productBuffer = xdr.readFixedOpaque(productName);
                long buildTimestampSeconds = xdr.readLong();
                int buildTimestampNanoseconds = xdr.readInt();
            }

            // Build the EXCHANGE_ID response
            Buffer response = buildExchangeIdReply(xid, data, seqid);
            //socket.write(response);
            return response;
        } catch (Exception e) {
            log.error("Error handling EXCHANGE_ID operation: ", e);
            sendRpcError(xid, nfsRequestContext.getSocket(), RpcConstants.GARBAGE_ARGS);
            // Ensure we always return a Buffer to satisfy the method contract.
            return Buffer.buffer();
        }
    }

    private Buffer buildExchangeIdReply(int requestXid, Buffer majorId, int seqid) {

        Buffer replyBuffer = Buffer.buffer();

        XdrUtils.writeLong(replyBuffer, CLIENT_COUNTER.get());
        XdrUtils.writeInt(replyBuffer, seqid); // Placeholder for seqid
        XdrUtils.writeInt(replyBuffer, 0x00020001); 
        XdrUtils.writeInt(replyBuffer, 0);
        XdrUtils.writeLong(replyBuffer, 0);    // Minor ID
        XdrUtils.writeOpaque(replyBuffer, majorId.getBytes()); // Major ID
        XdrUtils.writeOpaque(replyBuffer, majorId.getBytes()); // Server Scope
        XdrUtils.writeInt(replyBuffer, 0); // sei_impl_id4 length = 0

        return replyBuffer;
    }

    private Buffer handleCreateSession(NfsRequestContext nfsRequestContext) {
        int xid = nfsRequestContext.getXid();
        XdrDecodingStream xdr = nfsRequestContext.getXdr();

        try {
            // Decode EXCHANGE_ID arguments (simplified for now)
            long clientid = xdr.readLong(); // Skip EXCHANGE_ID arguments for now
            int seqid = xdr.readInt();
            int csa_flags = xdr.readInt();
            int csa_fore_chan_attrs_hdr_pad_size = xdr.readInt();
            int csa_fore_chan_attrs_max_req_size = xdr.readInt();
            int csa_fore_chan_attrs_max_resq_size = xdr.readInt();
            int csa_fore_chan_attrs_max_resq_size_cached = xdr.readInt();
            int csa_fore_chan_attrs_max_ops = xdr.readInt();
            int csa_fore_chan_attrs_max_reqs = xdr.readInt();
            int csa_back_chan_attrs_hdr_pad_size = xdr.readInt();
            int csa_back_chan_attrs_max_req_size = xdr.readInt();
            int csa_back_chan_attrs_max_resq_size = xdr.readInt();
            int csa_back_chan_attrs_max_resq_size_cached = xdr.readInt();
            int csa_back_chan_attrs_max_ops = xdr.readInt();
            int csa_back_chan_attrs_max_reqs = xdr.readInt();
            int cb_program = xdr.readInt();
            int flavor = xdr.readInt();
            int stamp = xdr.readInt();
            int machine_name_length = xdr.readInt();
            Buffer machine_name = xdr.readFixedOpaque(machine_name_length);
            int uid = xdr.readInt();
            int gid = xdr.readInt();

            // Build the EXCHANGE_ID response
            Buffer response = buildCreateSessionReply(clientid, seqid);
            //socket.write(response);
            return response;
        } catch (Exception e) {
            log.error("Error handling EXCHANGE_ID operation: ", e);
            sendRpcError(xid, nfsRequestContext.getSocket(), RpcConstants.GARBAGE_ARGS);
            // Ensure we always return a Buffer to satisfy the method contract.
            return Buffer.buffer();
        }
    }

    private static final AtomicLong CLIENT_COUNTER = new AtomicLong(0xea1f25695cfc63e8L);

    private Buffer buildCreateSessionReply(long clientId, int seqid) {
        // 定义前向通道属性 (原代码中的第一组数据)
        ChannelAttributes foreChannel = new ChannelAttributes(
                0,          // headerPadSize
                1049620,    // maxRequestSize
                1049480,    // maxResponseSize
                2128,       // maxResponseSizeCached
                8,          // maxOperations
                30          // maxRequests
        );

        // 定义后向通道属性 (原代码中的第二组数据)
        ChannelAttributes backChannel = new ChannelAttributes(
                0,          // headerPadSize
                4096,       // maxRequestSize
                4096,       // maxResponseSize
                0,          // maxResponseSizeCached
                2,          // maxOperations
                16          // maxRequests
        );

        // 构建完整响应
        CreateSessionReply reply = new CreateSessionReply(
                clientId, 1L,     // Session ID
                seqid,      // Sequence ID
                CreateSessionReply.CSR4_PERSIST, // Flags (原代码中的 0x00000002)
                foreChannel,
                backChannel
        );

        return reply.encode();
    }


    private Buffer handleSequence(NfsRequestContext nfsRequestContext) {
        int xid = nfsRequestContext.getXid();
        XdrDecodingStream xdr = nfsRequestContext.getXdr();
        Nfsv4Server.Nfsv4ConnectionState state = nfsRequestContext.getState();

        log.info("Handling NFSv4 NFS4_SEQUENCE operation for xid: {}", Integer.toUnsignedString(xid));
        try {
            // Decode EXCHANGE_ID arguments (simplified for now)
            long sessionIdHigh = xdr.readLong(); // Skip EXCHANGE_ID arguments for now
            long sessionIdLow = xdr.readLong();
            int seqid = xdr.readInt();
            int slotid = xdr.readInt();
            int highestSlotid = xdr.readInt();
            int cacheThis = xdr.readInt();

            state.sessionIdHigh = sessionIdHigh;
            state.sessionIdLow = sessionIdLow;
            state.slotid = slotid;
            state.highestSlotid = highestSlotid;
            state.cachethis = cacheThis;

            // Build the EXCHANGE_ID response
            Buffer response = buildSequenceReply(sessionIdHigh, sessionIdLow, seqid, slotid, 29);
            //socket.write(response);
            return response;
        } catch (Exception e) {
            log.error("Error handling EXCHANGE_ID operation: ", e);
            sendRpcError(xid, nfsRequestContext.getSocket(), RpcConstants.GARBAGE_ARGS);
            // Ensure we always return a Buffer to satisfy the method contract.
            return Buffer.buffer();
        }
    }

    private Buffer buildSequenceReply(long sessionIdHigh, long sessionIdLow, int seqid, int slotid, int highestSlotid) {
        // EXCHANGE_ID specific response payload
        Buffer replyBuffer = Buffer.buffer();

        // EID4_SERVER_OWNER
        XdrUtils.writeLong(replyBuffer, sessionIdHigh); // sessionid high
        XdrUtils.writeLong(replyBuffer, sessionIdLow); // sessionid low
        XdrUtils.writeInt(replyBuffer, seqid);
        XdrUtils.writeInt(replyBuffer, slotid); 
        XdrUtils.writeInt(replyBuffer, highestSlotid); // hdr pad
        XdrUtils.writeInt(replyBuffer, highestSlotid);    
        XdrUtils.writeInt(replyBuffer, 0);

        return replyBuffer;
    }

    private Buffer handlePutfhOperation(NfsRequestContext nfsRequestContext) {
        int xid = nfsRequestContext.getXid();
        XdrDecodingStream xdr = nfsRequestContext.getXdr();
        Nfsv4Server.Nfsv4ConnectionState state = nfsRequestContext.getState();
        log.info("Handling NFSv4 PUTFH operation for xid: {}", Integer.toUnsignedString(xid));
        try {
            int fhLength = xdr.readInt();
            Buffer fileHandle = xdr.readFixedOpaque(fhLength);
            BigInteger bigInt = new BigInteger(1, fileHandle.getBytes());
            long inodeId = bigInt.longValue();
            state.setCurrentInodeId(inodeId);
            log.info("PUTFH: Current Inode ID set to: {}", inodeId);
            return Buffer.buffer(); // Success, no specific data for PUTFH reply
        } catch (Exception e) {
            log.error("Error handling PUTFH operation: ", e);
            sendRpcError(xid, nfsRequestContext.getSocket(), RpcConstants.GARBAGE_ARGS);
            return Buffer.buffer();
        }
    }

    private Buffer handlePutrootfhOperation(NfsRequestContext nfsRequestContext) {
        int xid = nfsRequestContext.getXid();
        Nfsv4Server.Nfsv4ConnectionState state = nfsRequestContext.getState();
        log.info("Handling NFSv4 PUTROOTFH operation for xid: {}", Integer.toUnsignedString(xid));
        try {
            // The root filehandle's inode ID can be a predefined value or derived from the connection state.
            // For simplicity, let's use the targetVnodeId as the root inode ID.
            long rootInodeId = NfsConstants.ROOT_INODE_ID; // Or a specific UUID for the root
            state.setCurrentInodeId(rootInodeId);
            log.info("PUTROOTFH: Current Inode ID set to: {}", rootInodeId);
            return Buffer.buffer(); // Success, no specific data for PUTROOTFH reply
        } catch (Exception e) {
            log.error("Error handling PUTROOTFH operation: ", e);
            sendRpcError(xid, nfsRequestContext.getSocket(), RpcConstants.GARBAGE_ARGS);
            return Buffer.buffer();
        }
    }

    private Buffer handleNfs4Access(NfsRequestContext nfsRequestContext) {
        XdrDecodingStream xdr = nfsRequestContext.getXdr();
        int xid = nfsRequestContext.getXid();
        Nfsv4Server.Nfsv4ConnectionState state = nfsRequestContext.getState();

        // 1. Parse access_request bitmap
        int accessRequest = xdr.readInt();
        log.info("ACCESS request - xid: {}, access_request: 0x{}", Integer.toUnsignedString(xid), Integer.toHexString(accessRequest));

        Buffer rpcBodyBuffer = Buffer.buffer();

        // 2. Get current inode
        long currentInodeId = state.getCurrentInodeId();
        if (currentInodeId == 0) {
            XdrUtils.writeInt(rpcBodyBuffer, Nfs4Errors.NFS4ERR_NOFILEHANDLE);
            return rpcBodyBuffer;
        }

        Optional<Inode> inodeOpt = currentInodeId != NfsConstants.ROOT_INODE_ID ? MyRocksDB.getINodeMetaData(state.targetVnodeId, state.bucket, currentInodeId) : Optional.of(ROOT_INODE);
        if (!inodeOpt.isPresent()) {
            log.warn("ACCESS: Inode not found for ID: {}", currentInodeId);
            XdrUtils.writeInt(rpcBodyBuffer, Nfs4Errors.NFS4ERR_BADHANDLE);
            return rpcBodyBuffer;
        }

        Inode inode = inodeOpt.get();

        // 3. Determine access_supported and access_allowed
        // For simplicity, we'll assume all basic access bits are supported.
        // In a real implementation, this would depend on the filesystem capabilities.
        int accessSupported = Nfs4Access.ACCESS4_READ |
                              Nfs4Access.ACCESS4_LOOKUP |
                              Nfs4Access.ACCESS4_MODIFY |
                              Nfs4Access.ACCESS4_EXTEND |
                              Nfs4Access.ACCESS4_DELETE |
                              Nfs4Access.ACCESS4_EXECUTE;

        int accessAllowed = 0;

        // Check permissions based on inode mode
        int inodeMode = inode.getMode();

        // ACCESS4_READ: read data, read directory
        if ((accessRequest & Nfs4Access.ACCESS4_READ) != 0) {
            if ((inodeMode & Nfs4UnixMode.S_IRUSR) != 0) { // Owner read permission
                accessAllowed |= Nfs4Access.ACCESS4_READ;
            }
        }

        // ACCESS4_LOOKUP: look up a name in a directory (for directories only)
        if ((accessRequest & Nfs4Access.ACCESS4_LOOKUP) != 0) {
            if (inode.isDir() && (inodeMode & Nfs4UnixMode.S_IXUSR) != 0) { // Owner execute/search permission
                accessAllowed |= Nfs4Access.ACCESS4_LOOKUP;
            }
        }

        // ACCESS4_MODIFY: write data, create, setattr, link, symlink
        if ((accessRequest & Nfs4Access.ACCESS4_MODIFY) != 0) {
            if ((inodeMode & Nfs4UnixMode.S_IWUSR) != 0) { // Owner write permission
                accessAllowed |= Nfs4Access.ACCESS4_MODIFY;
            }
        }

        // ACCESS4_EXTEND: append data, create
        if ((accessRequest & Nfs4Access.ACCESS4_EXTEND) != 0) {
            if ((inodeMode & Nfs4UnixMode.S_IWUSR) != 0) { // Owner write permission
                accessAllowed |= Nfs4Access.ACCESS4_EXTEND;
            }
        }

        // ACCESS4_DELETE: delete a file or directory entry
        if ((accessRequest & Nfs4Access.ACCESS4_DELETE) != 0) {
            // For simplicity, allow delete if parent directory is writable (not checked here)
            // and the file itself is writable by owner.
            if ((inodeMode & Nfs4UnixMode.S_IWUSR) != 0) { // Owner write permission
                accessAllowed |= Nfs4Access.ACCESS4_DELETE;
            }
        }

        // ACCESS4_EXECUTE: execute a file or search a directory
        if ((accessRequest & Nfs4Access.ACCESS4_EXECUTE) != 0) {
            if ((inodeMode & Nfs4UnixMode.S_IXUSR) != 0) { // Owner execute permission
                accessAllowed |= Nfs4Access.ACCESS4_EXECUTE;
            }
        }

        // Only return the flags that were requested and are allowed.
        accessAllowed &= accessRequest;

        log.info("ACCESS response - xid: {}, access_supported: 0x{}, access_allowed: 0x{}",
                Integer.toUnsignedString(xid), Integer.toHexString(accessSupported), Integer.toHexString(accessAllowed));

        // 4. Build response
        XdrUtils.writeInt(rpcBodyBuffer, Nfs4Errors.NFS4_OK); // Operation status
        XdrUtils.writeInt(rpcBodyBuffer, accessSupported); // access_supported
        XdrUtils.writeInt(rpcBodyBuffer, accessAllowed);   // access_allowed

        return rpcBodyBuffer;
    }

    private Buffer handleNfs4Readdir(NfsRequestContext nfsRequestContext) {
        XdrDecodingStream xdr = nfsRequestContext.getXdr();
        int xid = nfsRequestContext.getXid();
        Nfsv4Server.Nfsv4ConnectionState state = nfsRequestContext.getState();

        Buffer rpcBodyBuffer = Buffer.buffer();

        try {
            // 1. Decode READDIR arguments
            // stateid4 (seqid, other[12])
            long cookie = xdr.readLong(); // cookie4
            long cookieVerf = xdr.readLong(); // cookieverf4
            int dircount = xdr.readInt(); // dircount
            int maxcount = xdr.readInt(); // maxcount (byte limit for response)
            int attrBitmapLen = xdr.readInt(); // attr_request bitmap length
            int[] reqBitmaps = new int[attrBitmapLen];
            for (int i = 0; i < attrBitmapLen; i++) {
                reqBitmaps[i] = xdr.readInt();
            }

            log.info("READDIR request - xid: {}, cookie: {}, cookie_verf: {}, dircount: {}, maxcount: {}, attr_request[0]: 0x{}",
                    Integer.toUnsignedString(xid), cookie, cookieVerf, dircount, maxcount, attrBitmapLen > 0 ? Integer.toHexString(reqBitmaps[0]) : "N/A");

            // 2. Get current inode (which must be a directory)
            long currentInodeId = state.getCurrentInodeId();
            if (currentInodeId == 0) {
                XdrUtils.writeInt(rpcBodyBuffer, Nfs4Errors.NFS4ERR_NOFILEHANDLE);
                return rpcBodyBuffer;
            }

            Optional<Inode> inodeOpt = currentInodeId != NfsConstants.ROOT_INODE_ID ? MyRocksDB.getINodeMetaData(state.targetVnodeId, state.bucket, currentInodeId) : Optional.of(ROOT_INODE);
            if (!inodeOpt.isPresent()) {
                log.warn("READDIR: Inode not found for ID: {}", currentInodeId);
                XdrUtils.writeInt(rpcBodyBuffer, Nfs4Errors.NFS4ERR_BADHANDLE);
                return rpcBodyBuffer;
            }

            Inode parentInode = inodeOpt.get();
            if (!parentInode.isDir()) {
                log.warn("READDIR: Inode {} is not a directory.", currentInodeId);
                XdrUtils.writeInt(rpcBodyBuffer, Nfs4Errors.NFS4ERR_NOTDIR);
                return rpcBodyBuffer;
            }

            // 3. Prepare response buffer and write cookieverf
            Buffer readdirBodyBuffer = Buffer.buffer();
            // For simplicity, generate a new cookieverf each time. In a real system, it should change only on directory modification.
            long newCookieVerf = System.nanoTime();

            // 4. Get directory entries from MyRocksDB
            // Assuming currentInodeId corresponds to a directory path
            String directoryPath = parentInode.getObjName(); // Assuming Inode.objName stores the full path
            List<LatestIndexMetadata> entries = MyRocksDB.listDirectoryContents(state.targetVnodeId, state.bucket, directoryPath);

            boolean eof = true; 
            int bytesWritten = 0; // Track bytes written to respect maxcount
            int entryCount = 0;   // Track number of entries written to respect dircount

            // Filter entries based on cookie for pagination
            List<LatestIndexMetadata> filteredEntries = entries.stream()
                    .filter(entry -> entry.getInode() > cookie) // Only include entries with inode ID greater than the cookie
                    .sorted(Comparator.comparing(LatestIndexMetadata::getInode)) // Ensure consistent ordering
                    .collect(Collectors.toList());
            
            // Loop through filtered entries and add them to the response
            for (int i = 0; i < filteredEntries.size(); i++) {
                LatestIndexMetadata entryMetadata = filteredEntries.get(i);

                // Create a temporary buffer for this entry to check its size before adding
                Buffer tempEntryBuffer = Buffer.buffer();

                // Determine next_cookie for this entry
                long currentEntryNextCookie;
                if (i == filteredEntries.size() - 1) {
                    // If this is the last entry in the filtered list, and if it's truly the end of the directory,
                    // set the special cookie. Otherwise, it's the next inode ID.
                    currentEntryNextCookie = eof ? 0x7fffffffffffffffL : entryMetadata.getInode() + 1L;
                } else {
                    currentEntryNextCookie = filteredEntries.get(i + 1).getInode();
                }

                // entry4: next_cookie, fileid, name, attrs
                // First, write value_follows for this entry
                boolean entryValueFollows = true; // Always true for entries within the list, until the last one when overall eof is determined.
                XdrUtils.writeBoolean(tempEntryBuffer, entryValueFollows);

                XdrUtils.writeLong(tempEntryBuffer, currentEntryNextCookie); // next_cookie
                //XdrUtils.writeLong(tempEntryBuffer, entryMetadata.getInode()); // fileid4

                // name4
                // Extract the file name from the full path (entryMetadata.getKey())
                Path entryPath = Paths.get(entryMetadata.getKey());
                String fileName = entryPath.getFileName().toString();
                XdrUtils.writeXdrString(tempEntryBuffer, fileName);
                //XdrUtils.writeXdrPad(tempEntryBuffer, nameBytes.length); // Padding

                // attrs (attrlist4)
                // Retrieve the Inode for this entry to build attributes
                Optional<Inode> entryInodeOpt = MyRocksDB.getINodeMetaData(state.targetVnodeId, state.bucket, entryMetadata.getInode());
                if (entryInodeOpt.isPresent()) {
                    Buffer entryAttrBuffer = NfsFileAttributes.build(entryInodeOpt.get(), reqBitmaps, nfsRequestContext);
                    tempEntryBuffer.appendBuffer(entryAttrBuffer);
                } else {
                    // If inode not found, return empty attributes or an error. For now, empty.
                    // This case should ideally not happen if listDirectoryContents returns valid entries.
                    XdrUtils.writeInt(tempEntryBuffer, 0); // attr_bitmap.length = 0
                    XdrUtils.writeInt(tempEntryBuffer, 0); // attr_list.length = 0
                }

                // Check if adding this entry exceeds maxcount or dircount
                if (bytesWritten + tempEntryBuffer.length() > maxcount || entryCount >= dircount) {
                    eof = false; // Not EOF, more entries exist but we hit limits
                    break;
                }

                readdirBodyBuffer.appendBuffer(tempEntryBuffer);
                bytesWritten += tempEntryBuffer.length();
                entryCount++;
            }
            
            // After the loop, determine the overall eof status for the dirlist
            boolean finalEof = eof && (entryCount == filteredEntries.size());

            // Build the final READDIR4resok structure in rpcBodyBuffer
            // 1. Operation status
            //XdrUtils.writeInt(rpcBodyBuffer, Nfs4Errors.NFS4_OK);
            // 2. cookieverf4
            XdrUtils.writeLong(rpcBodyBuffer, newCookieVerf);
            // 3. dirlist4 (contains entries and final eof)
            rpcBodyBuffer.appendBuffer(readdirBodyBuffer); // Append the entries collected in readdirBodyBuffer
            // 4. final eof (for the whole dirlist)
            XdrUtils.writeBoolean(rpcBodyBuffer, false); // Overall eof for the dirlist
            XdrUtils.writeBoolean(rpcBodyBuffer, finalEof); // Overall eof for the dirlist

            log.info("READDIR response - xid: {}, entries returned: {}, finalEof: {}",
                    Integer.toUnsignedString(xid), entryCount, finalEof);

            return rpcBodyBuffer;

        } catch (Exception e) {
            log.error("Error handling READDIR operation: ", e);
            XdrUtils.writeInt(rpcBodyBuffer, Nfs4Errors.NFS4ERR_SERVERFAULT);
            return rpcBodyBuffer;
        }
    }

    private Buffer handleNfs4Read(NfsRequestContext nfsRequestContext) {
        XdrDecodingStream xdr = nfsRequestContext.getXdr();
        int xid = nfsRequestContext.getXid();
        Nfsv4Server.Nfsv4ConnectionState state = nfsRequestContext.getState();

        Buffer rpcBodyBuffer = Buffer.buffer();

        try {
            // 1. Decode READ arguments: stateid4, offset4, count4
            // stateid4 (seqid, other[12])
            int stateidSeqid = xdr.readInt();
            Buffer stateidother = xdr.readFixedOpaque(12); // Skip stateid.other
            long offset = xdr.readLong(); // offset4
            int count = xdr.readInt();   // count4

            log.info("READ request - xid: {}, stateid_seqid: {}, offset: {}, count: {}",
                    Integer.toUnsignedString(xid), stateidSeqid, offset, count);

            // 2. Get current inode
            long currentInodeId = state.getCurrentInodeId();
            if (currentInodeId == 0) {
                XdrUtils.writeInt(rpcBodyBuffer, Nfs4Errors.NFS4ERR_NOFILEHANDLE);
                return rpcBodyBuffer;
            }

            Optional<Inode> inodeOpt = currentInodeId != NfsConstants.ROOT_INODE_ID ? MyRocksDB.getINodeMetaData(state.targetVnodeId, state.bucket, currentInodeId) : Optional.of(ROOT_INODE);
            if (!inodeOpt.isPresent()) {
                log.warn("READ: Inode not found for ID: {}", currentInodeId);
                XdrUtils.writeInt(rpcBodyBuffer, Nfs4Errors.NFS4ERR_BADHANDLE);
                return rpcBodyBuffer;
            }

            Inode inode = inodeOpt.get();

            // Check if it's a directory
            if (inode.isDir()) {
                log.warn("READ: Inode {} is a directory.", currentInodeId);
                XdrUtils.writeInt(rpcBodyBuffer, Nfs4Errors.NFS4ERR_ISDIR);
                return rpcBodyBuffer;
            }

            // 3. Read data from underlying storage
            // This will involve reading from FILE_DATA_DEVICE_PATH at the calculated offset.
            Buffer dataBuffer = Buffer.buffer();
            long bytesToRead = Math.min(count, inode.getSize() - offset);
            boolean eofFlag = false;

            if (bytesToRead > 0) {
                // Logic to read from disk based on fileMetadata and offset/count
                // For simplicity, this is a placeholder. Real implementation needs to handle block allocation.
                // Assuming fileMetadata.getOffset() and fileMetadata.getLen() give us the block mapping.
                byte[] fileContent = "hello,world12345567890".getBytes(StandardCharsets.UTF_8);
                if (fileContent != null) {
                    dataBuffer.appendBytes(fileContent);
                }
            }

            if (offset + dataBuffer.length() >= inode.getSize()) {
                eofFlag = true;
            }

            // 4. Build READ4resok response
            XdrUtils.writeInt(rpcBodyBuffer, Nfs4Errors.NFS4_OK); // Operation status
            // stateid4 (seqid, other[12]) - reuse request stateid for now
            XdrUtils.writeInt(rpcBodyBuffer, stateidSeqid);
            // For stateid.other, write 12 bytes of zeros as a placeholder
            XdrUtils.writeOpaque(rpcBodyBuffer, new byte[12]); // Placeholder for stateid.other
            XdrUtils.writeBoolean(rpcBodyBuffer, eofFlag); // eof
            XdrUtils.writeOpaque(rpcBodyBuffer, dataBuffer.getBytes()); // data

            return rpcBodyBuffer;

        } catch (Exception e) {
            log.error("Error handling READ operation: ", e);
            XdrUtils.writeInt(rpcBodyBuffer, Nfs4Errors.NFS4ERR_SERVERFAULT);
            return rpcBodyBuffer;
        }
    }

    private Buffer handleNfs4Open(NfsRequestContext nfsRequestContext) {
        XdrDecodingStream xdr = nfsRequestContext.getXdr();
        int xid = nfsRequestContext.getXid();
        Nfsv4Server.Nfsv4ConnectionState state = nfsRequestContext.getState();

        Buffer rpcBodyBuffer = Buffer.buffer();

        try {
            // 1. Decode OPEN arguments
            int seqid = xdr.readInt(); // seqid4
            xdr.readFixedOpaque(16); // stateid4 (current filehandle state ID: 4 bytes seqid, 12 bytes other)
            // open_owner4 (clientid, ownerid)
            long clientId = xdr.readLong(); // clientid4
            int ownerIdLen = xdr.readInt();
            Buffer ownerIdBuffer = xdr.readFixedOpaque(ownerIdLen);
            String ownerId = ownerIdBuffer.toString(StandardCharsets.UTF_8);
            state.setOwnerId(ownerId);

            int shareAccess = xdr.readInt(); // share_access4
            int shareDeny = xdr.readInt();   // share_deny4
            int openCreateType = xdr.readInt(); // open_create_type4 (NONE, UNCHECKED, GUARDED, EXCLUSIVE)

            // If openCreateType is not OPEN4_CREATE_NONE, then create_how4 follows
            int createMode = 0;
            int attrBitmapLen = 0;
            int[] createAttrsBitmap = null;
            if (openCreateType == Nfs4OpenConstants.OPEN4_CREATE) { // This now correctly checks if it's a create operation
                createMode = xdr.readInt(); // create_mode4 (UNCHECKED, GUARDED, EXCLUSIVE)
                // If createMode is UNCHECKED or GUARDED, then attrs4 follows
                if (createMode == Nfs4OpenConstants.OPEN4_CREATE_UNCHECKED || createMode == Nfs4OpenConstants.OPEN4_CREATE_GUARDED) {
                    attrBitmapLen = xdr.readInt();
                    createAttrsBitmap = new int[attrBitmapLen];
                    for (int i = 0; i < attrBitmapLen; i++) {
                        createAttrsBitmap[i] = xdr.readInt();
                    }
                    // Skip actual attributes for now (will implement proper attribute decoding later)
                    // For simplicity, assume no complex attributes are sent for now, or just skip their length.
                    // The client might send attr_len after the bitmap.
                    // TODO: Properly skip/decode create attributes.
                } else if (createMode == Nfs4OpenConstants.OPEN4_CREATE_EXCLUSIVE) {
                    // exclusive4 (verifier)
                    xdr.readLong(); // verifier, skip for now
                }
            } else if (openCreateType == Nfs4OpenConstants.OPEN4_NOCREATE) {
                // No create_how4 for OPEN4_NOCREATE
            } else {
                // This should be an error case, open_create_type4 is invalid
                XdrUtils.writeInt(rpcBodyBuffer, Nfs4Errors.NFS4ERR_INVAL);
                return rpcBodyBuffer;
            }

            int claimType = xdr.readInt(); // claim_type4
            String fileName = "";
            if (claimType == Nfs4OpenConstants.OPEN4_CLAIM_NULL) {
                int nameLen = xdr.readInt();
                fileName = xdr.readFixedOpaque(nameLen).toString(StandardCharsets.UTF_8);
            }
            // TODO: Handle other claim types (CLAIM_FH, CLAIM_DELEGATE_CUR, etc.)

            log.info("OPEN request - xid: {}, seqid: {}, clientid: {}, ownerid: {}, share_access: 0x{}, share_deny: 0x{}, open_create_type: {}, create_mode: {}, claim_type: {}, name: {}",
                    Integer.toUnsignedString(xid), seqid, clientId, ownerId, Integer.toHexString(shareAccess), Integer.toHexString(shareDeny), openCreateType, createMode, claimType, fileName);

            // File lookup/creation logic
            long targetInodeId = state.getCurrentInodeId();
            String parentPath = "/";
            if (targetInodeId != NfsConstants.ROOT_INODE_ID) {
                Optional<Inode> parentInodeOpt = MyRocksDB.getINodeMetaData(state.targetVnodeId, state.bucket, targetInodeId);
                if (!parentInodeOpt.isPresent()) {
                    XdrUtils.writeInt(rpcBodyBuffer, Nfs4Errors.NFS4ERR_BADHANDLE);
                    return rpcBodyBuffer;
                }
                parentPath = parentInodeOpt.get().getObjName();
            }

            String fullPath = (parentPath.equals("/") ? "/" : parentPath + "/") + fileName;
            VersionIndexMetadata  existingMetadata = MyRocksDB.getIndexMetaData(state.targetVnodeId, state.bucket, fullPath).orElseThrow(() -> {
                throw new RuntimeException("Failed to retrieve index metadata for path: " + fullPath);
            }); // Use new method

            Optional<Inode> existingInodeOpt = MyRocksDB.getINodeMetaData(state.targetVnodeId, state.bucket, existingMetadata.getInode()); // Use new method

            int status = Nfs4Errors.NFS4_OK;
            int rflags = 0; // Result flags
            long beforeChange = 0L; // For change_info4
            long afterChange = 0L;  // For change_info4
            long newInodeId = 0L;
            Inode openedInode = null;

            if (openCreateType == Nfs4OpenConstants.OPEN4_CREATE) {
                // Client wants to create a file
                if (createMode == Nfs4OpenConstants.OPEN4_CREATE_GUARDED && existingInodeOpt.isPresent()) {
                    status = Nfs4Errors.NFS4ERR_EXIST;
                } else if (createMode == Nfs4OpenConstants.OPEN4_CREATE_EXCLUSIVE && existingInodeOpt.isPresent()) {
                    // For exclusive create, if file exists, it's an error (EEXIST)
                    status = Nfs4Errors.NFS4ERR_EXIST;
                } else if (existingInodeOpt.isPresent()) {
                    // OPEN4_CREATE_UNCHECKED or implicit create, and file exists
                    // Treat as opening an existing file. For now, no truncation.
                    openedInode = existingInodeOpt.get();
                    newInodeId = openedInode.getNodeId();
                    beforeChange = openedInode.getMtime(); // Use mtime as change value
                    afterChange = System.currentTimeMillis();
                    rflags |= Nfs4OpenConstants.OPEN4_RESULT_FILE_EXISTS;
                    status = Nfs4Errors.NFS4_OK; // Existing file, successfully opened.
                } else {
                    // Create new file
                    // For simplicity, create as a regular file with default permissions.
                    // TODO: Apply createAttrsBitmap if provided
                    Optional<Inode> createdInode = MyRocksDB.saveIndexMetaAndInodeData(state.targetVnodeId, state.bucket, fullPath, 0L, "application/octet-stream", Inode.Mode.S_IFREG.getCode() | 0644);
                    if (createdInode.isPresent()) {
                        openedInode = createdInode.get();
                        newInodeId = openedInode.getNodeId();
                        afterChange = System.currentTimeMillis();
                        rflags |= Nfs4OpenConstants.OPEN4_RESULT_FILE_CREATED;
                        status = Nfs4Errors.NFS4_OK; // File created and successfully opened.
                    } else {
                        status = Nfs4Errors.NFS4ERR_IO; // Failed to create inode
                    }
                }
            } else if (openCreateType == Nfs4OpenConstants.OPEN4_NOCREATE) {
                // Client wants to open an existing file, fail if not found
                if (existingInodeOpt.isPresent()) {
                    openedInode = existingInodeOpt.get();
                    newInodeId = openedInode.getNodeId();
                    beforeChange = openedInode.getMtime();
                    afterChange = System.currentTimeMillis();
                    status = Nfs4Errors.NFS4_OK; // Existing file, successfully opened.
                } else {
                    status = Nfs4Errors.NFS4ERR_NOENT;
                }
            } else {
                status = Nfs4Errors.NFS4ERR_INVAL; // Should have been caught earlier, but just in case
            }

            // Build the OPEN4resok response
            XdrUtils.writeInt(rpcBodyBuffer, status); // Status
            if (status == Nfs4Errors.NFS4_OK) {
                // open_resok4
                // Generate a new stateid4 for this open operation
                Nfs4StateId newOpenStateId = Nfs4StateId.generateNew(seqid); // Use request seqid for new stateid seqid
                state.addOpenedFileState(openedInode.getNodeId(), newOpenStateId);

                // stateid4 (new open state ID)
                XdrUtils.writeInt(rpcBodyBuffer, newOpenStateId.getSeqid()); // seqid
                XdrUtils.writeOpaque(rpcBodyBuffer, newOpenStateId.getOther()); // other
                // change_info4 (before, after)
                XdrUtils.writeBoolean(rpcBodyBuffer, true); // atomic
                XdrUtils.writeLong(rpcBodyBuffer, beforeChange); // before
                XdrUtils.writeLong(rpcBodyBuffer, afterChange); // after
                // rflags4
                XdrUtils.writeInt(rpcBodyBuffer, rflags | Nfs4OpenConstants.OPEN4_RESULT_CONFIRM); // Always request confirmation for now
                // attrlist4 (empty for now). In a real implementation, this would return attributes.
                XdrUtils.writeInt(rpcBodyBuffer, 0); // bitmap length = 0
                XdrUtils.writeInt(rpcBodyBuffer, 0); // attr list length = 0
                // delegation4 (none for now)
                XdrUtils.writeInt(rpcBodyBuffer, 0); // No delegation (DELEG_NONE = 0)
            }

            return rpcBodyBuffer;

        } catch (Exception e) {
            log.error("Error handling OPEN operation: ", e);
            XdrUtils.writeInt(rpcBodyBuffer, Nfs4Errors.NFS4ERR_SERVERFAULT);
            return rpcBodyBuffer;
        }
    }

    private Buffer getReplyHeader(int requestXid) {
        Buffer rpcHeaderBuffer = Buffer.buffer();

        rpcHeaderBuffer.appendInt(requestXid);
        rpcHeaderBuffer.appendInt(RpcConstants.REPLY);
        rpcHeaderBuffer.appendInt(RpcConstants.MSG_ACCEPTED);
        rpcHeaderBuffer.appendInt(RpcConstants.AUTH_NONE);
        rpcHeaderBuffer.appendInt(0);
        rpcHeaderBuffer.appendInt(RpcConstants.SUCCESS);

        return rpcHeaderBuffer;
    }

    // Helper to send generic RPC errors
    private void sendRpcError(int xid, NetSocket socket, int acceptStat) {
        log.debug("Sending RPC error: xid={}, acceptStat={}", Integer.toUnsignedString(xid), acceptStat);
        Buffer response = buildRpcReply(xid, RpcConstants.MSG_ACCEPTED, acceptStat);
        socket.write(response);
    }

    /**
     * Helper to build a basic RPC reply message using Vert.x Buffer.
     *
     * @param requestXid The transaction identifier from the original request.
     * @return A Vert.x Buffer containing the complete XDR response.
     */
    private Buffer buildRpcReply(int requestXid, int msgAccepted, int acceptStat) {
        // --- Create Buffer for the RPC Message Body ---
        // Vert.x Buffer will automatically expand, so we can just append data.
        // It's Big Endian by default, which is what XDR requires.
        Buffer rpcBodyBuffer = Buffer.buffer();

        // 1. XID (Transaction Identifier) - from request
        rpcBodyBuffer.appendInt(requestXid);

        // 2. Message Type (mtype)
        rpcBodyBuffer.appendInt(RpcConstants.REPLY);

        // 3. Reply Body (reply_body)
        //    3.1. Reply Status (stat)
        rpcBodyBuffer.appendInt(msgAccepted);

        //    3.2. Accepted Reply (areply)
        //        3.2.1. Verifier (verf - opaque_auth structure)
        rpcBodyBuffer.appendInt(RpcConstants.AUTH_NONE); // Flavor
        rpcBodyBuffer.appendInt(0);      // Length of body (0 for AUTH_NONE)

        //        3.2.2. Acceptance Status (stat)
        rpcBodyBuffer.appendInt(acceptStat);

        //        3.2.3. Results (for NFSPROC3_NULL, this is void, so no data is appended)

        // --- Calculate RPC Message Body Length ---
        // This is simply the length of the buffer we just built.
        final int rpcMessageBodyLength = rpcBodyBuffer.length(); // 6 * 4 = 24 bytes

        // --- Construct Record Marking ---
        // Highest bit set (0x80000000) ORed with the length of the RPC message body.
        int recordMarkValue = 0x80000000 | rpcMessageBodyLength;

        // --- Create the Full XDR Response ---
        // Start with a new buffer for the final response.
        Buffer fullResponseBuffer = Buffer.buffer();

        // Prepend the record mark
        fullResponseBuffer.appendInt(recordMarkValue);

        // Append the RPC message body
        fullResponseBuffer.appendBuffer(rpcBodyBuffer);

        // Return the complete buffer
        return fullResponseBuffer;
    }

    public static class Nfsv4ConnectionState {
        String currentDirectory = "/";
        String bucket = "ftp_bucket"; // Dedicated bucket for NFSv4
        String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);
        String serverOwner; // Represents the unique identifier for the server in this session
        String clientMajorId; // Client's major ID from EXCHANGE_ID
        int clientMinorId; // Client's minor ID from EXCHANGE_ID
        long sessionIdHigh;
        long sessionIdLow;
        int slotid;
        int highestSlotid;
        int cachethis;
        long currentInodeId; // Added to store the inode ID of the current filehandle
        String ownerId; // Owner ID from OPEN operation

        // Map to store active open state IDs for inodes
        private final Map<Long, Nfs4StateId> openedFileStateIds = new ConcurrentHashMap<>();

        public void setServerOwner(String serverOwner) {
            this.serverOwner = serverOwner;
        }

        public String getServerOwner() {
            return serverOwner;
        }

        public void setClientMajorId(String clientMajorId) {
            this.clientMajorId = clientMajorId;
        }

        public String getClientMajorId() {
            return clientMajorId;
        }

        public void setClientMinorId(int clientMinorId) {
            this.clientMinorId = clientMinorId;
        }

        public int getClientMinorId() {
            return clientMinorId;
        }

        public void setOwnerId(String ownerId) {
            this.ownerId = ownerId;
        }

        public String getOwnerId() {
            return ownerId;
        }

        public void setCurrentInodeId(long currentInodeId) {
            this.currentInodeId = currentInodeId;
        }

        public long getCurrentInodeId() {
            return currentInodeId;
        }

        public void addOpenedFileState(long inodeId, Nfs4StateId stateId) {
            this.openedFileStateIds.put(inodeId, stateId);
        }

        public Optional<Nfs4StateId> getOpenedFileState(long inodeId) {
            return Optional.ofNullable(this.openedFileStateIds.get(inodeId));
        }
    }

    // 占位符：用于模拟 NFSv4 的 XDR 帧处理
    // 实际实现需要一个复杂的 XDR 解码器
    private static class NFSV4CommandFramer implements FlowableTransformer<Buffer, Buffer> {
        private RpcParseState currentState = RpcParseState.READING_MARKER;
        private boolean isLastFragment;
        private int expectedFragmentLength;
        private RecordParser parser;

        @Override
        public Publisher<Buffer> apply(Flowable<Buffer> upstream) {
            return Flowable.create(emitter -> {
                final AtomicReference<Buffer> fullMessageRef = new AtomicReference<>(Buffer.buffer());

                // Initialize parser for XDR framing: first 4 bytes is the record marker
                parser = RecordParser.newFixed(4);

                parser.handler(buffer -> {

                    if (currentState == RpcParseState.READING_MARKER) {
                        // We got the 4-byte record marker
                        long recordMarkerRaw = buffer.getUnsignedInt(0); // Read as unsigned integer
                        isLastFragment = (recordMarkerRaw & 0x80000000L) != 0;
                        expectedFragmentLength = (int) (recordMarkerRaw & 0x7FFFFFFFL); // Lower 31 bits are the length

                        // System.out.println("Parsed Marker: last=" + isLastFragment + ", length=" + expectedFragmentLength);

                        if (expectedFragmentLength == 0) { // Heartbeat or empty fragment
                            // Reset to read next marker (RecordParser automatically goes back to fixed(4))
                            parser.fixedSizeMode(4);
                            currentState = RpcParseState.READING_MARKER;
                        } else {
                            parser.fixedSizeMode(expectedFragmentLength); // Switch to reading fragment data mode
                            currentState = RpcParseState.READING_FRAGMENT_DATA;
                        }

                    } else if (currentState == RpcParseState.READING_FRAGMENT_DATA) {
                        // We got the fragment data
                        // System.out.println("Received fragment data of length: " + buffer.length());
                        Buffer completeMessage = fullMessageRef.get();
                        completeMessage.appendBuffer(buffer);

                        if (isLastFragment) {
                            //log.info("完整消息: {}", completeMessage.toString());
                            emitter.onNext(completeMessage); // Emit the complete RPC message
                            fullMessageRef.set(Buffer.buffer()); // Reset for the next message
                        }
                        // Whether it's the last fragment or not, the next should be a record marker
                        parser.fixedSizeMode(4); // Reset to read next marker
                        currentState = RpcParseState.READING_MARKER;
                    }
                });

                parser.exceptionHandler(emitter::onError);

                // 订阅上游的原始字节流，并将数据喂给 parser
                upstream.subscribe(
                        reactiveBuffer -> parser.handle(reactiveBuffer), // 同样需要 getDelegate()
                        emitter::onError,
                        emitter::onComplete
                );

            }, io.reactivex.BackpressureStrategy.BUFFER);
        }
    }

    private enum RpcParseState {
        READING_MARKER,
        READING_FRAGMENT_DATA
    }
}
