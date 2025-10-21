package com.mycompany.rocksdb.netserver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mycompany.rocksdb.POJO.ChunkFile;
import com.mycompany.rocksdb.POJO.Inode;
import com.mycompany.rocksdb.POJO.LatestIndexMetadata;
import com.mycompany.rocksdb.POJO.NFSFileHandle;
import com.mycompany.rocksdb.POJO.FileMetadata;
import com.mycompany.rocksdb.POJO.VersionIndexMetadata;
import com.mycompany.rocksdb.RPC.RpcHeader;
import com.mycompany.rocksdb.SimpleRSocketClient;
import com.mycompany.rocksdb.SocketReqMsg;
import com.mycompany.rocksdb.enums.Nfs3Constant;
import com.mycompany.rocksdb.enums.Nfs3Procedure;
import com.mycompany.rocksdb.enums.NfsStat3;
import com.mycompany.rocksdb.enums.RpcConstants;
import com.mycompany.rocksdb.model.*;
import com.mycompany.rocksdb.model.acl.GETACL3res;
import com.mycompany.rocksdb.model.acl.GETACL3resfail;
import com.mycompany.rocksdb.myrocksdb.MyRocksDB;
import com.mycompany.rocksdb.utils.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;
import lombok.Getter;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.mycompany.rocksdb.constant.GlobalConstant.BLOCK_SIZE;
import static com.mycompany.rocksdb.constant.GlobalConstant.ROCKS_CHUNK_FILE_KEY;
import static com.mycompany.rocksdb.utils.MetaKeyUtils.getRequestId;
import static com.mycompany.rocksdb.utils.NetTool.bytesToHex;
import static com.mycompany.rocksdb.utils.NetTool.bytesToHex2;


public class Nfsv3Server extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(MountServer.class);

    private static final int PORT = 12345; // 服务器监听的端口
    private static final String HOST = "0.0.0.0"; // 监听所有网络接口

    private RpcParseState currentState = RpcParseState.READING_MARKER;
    private int expectedFragmentLength;
    private boolean isLastFragment;
    private List<Buffer> messageFragments = new ArrayList<>();

    // NFS Program Constants
    private static final int NFS_PROGRAM = 100003;
    private static final int NFS_VERSION = 3;

    // NFS_ACL Program Constants
    private static final int NFS_ACL_PROGRAM = 100227;
    private static final int NFS_ACL_VERSION = 3;

    // NFS_ACL Procedure Numbers
    private static final int NFSPROC_ACL_NULL = 0;
    private static final int NFSPROC_ACL_GETACL = 1;
    private static final int NFSPROC_ACL_SETACL = 2;

    private static final Map<String, ByteArrayKeyWrapper> fileNameTofileHandle = new ConcurrentHashMap<>();
    private static final Map<String, Long> fileNameTofileId = new ConcurrentHashMap<>();
    private static final Map<ByteArrayKeyWrapper, Long> fileHandleToFileId = new ConcurrentHashMap<>();
    private static final Map<ByteArrayKeyWrapper, String> fileHandleToFileName = new ConcurrentHashMap<>();
    private static final Map<Long, String> fileIdToFileName = new ConcurrentHashMap<>();
    private static final Map<Long, FAttr3> fileIdToFAttr3 = new ConcurrentHashMap<>();
    private static final Map<ByteArrayKeyWrapper, FAttr3> fileHandleToFAttr3 = new ConcurrentHashMap<>();
    private static final Map<Long, FAttr3> inodeIdToFAttr3 = new ConcurrentHashMap<>();
    private static final Map<ByteArrayKeyWrapper, ByteArrayKeyWrapper> fileHandleToParentFileHandle = new ConcurrentHashMap<>();
    private static final Map<ByteArrayKeyWrapper, List<ByteArrayKeyWrapper>> fileHandleToChildrenFileHandle = new ConcurrentHashMap<>();
    private static final Map<ByteArrayKeyWrapper, String> fileHandleToRequestId = new ConcurrentHashMap<>();
    private static final Map<ByteArrayKeyWrapper, ConcurrentSkipListMap<Long, Long>> fileHandleToOffset= new ConcurrentHashMap<>();
    private static final Map<ByteArrayKeyWrapper, Inode> fileHandleToINode = new ConcurrentHashMap<>();

    private static final Map<ByteArrayKeyWrapper, AtomicLong> fileHandleNextAppendingPosition = new ConcurrentHashMap<>();

    private static final String STATIC_FILES_ROOT = "public";
    private static final String S3HOST = "172.20.123.124";
    private static final int RSOCKET_PORT = 7000;
    private static final String CONJUGATE_RSOCKET_PORT = "172.20.123.123";

    private static final String INDEX_LUN = "fs-SP0-8-index";
    private static final String DATA_LUN = "fs-SP0-2";
    private static final Path SAVE_DATA_PATH = Paths.get("/dev/sdg2");
    public static final String BUCK_NAME = "test";

    public static FAttr3 rootDirAttributes;

    static {
        // Current time in seconds and nanoseconds
        long currentTimeMillis = System.currentTimeMillis();
        int seconds = (int) (currentTimeMillis / 1000);
        int nseconds = (int) ((currentTimeMillis % 1000) * 1_000_000);
        rootDirAttributes = FAttr3.builder()
                .type(2)
                .mode(0755)
                .nlink(1)
                .uid(0)
                .gid(0)
                .size(4096)
                .used(4096)
                .rdev(0)
                .fsidMajor(0)
                .fsidMinor(1)
                .fileid(1)
                .atimeSeconds(seconds)
                .atimeNseconds(nseconds)
                .mtimeSeconds(seconds)
                .mtimeNseconds(nseconds)
                .ctimeSeconds(seconds)
                .ctimeNseconds(nseconds)
                .build();

    }
    @Override
    public void start(Future<Void> startFuture) throws Exception {

        // 创建 NetServerOptions (可选，用于配置服务器)
        NetServerOptions options = new NetServerOptions()
                .setPort(PORT)
                .setHost(HOST)
                .setTcpKeepAlive(true); // 示例：启用 TCP KeepAlive

        // 创建 TCP 服务器
        NetServer server = vertx.createNetServer(options);

        // 设置连接处理器
        server.connectHandler(socket -> {
            log.info("客户端连接成功: " + socket.remoteAddress());

            // RecordParser 会替我们处理 TCP 分片问题
            final RecordParser parser = RecordParser.newFixed(4); // Start by reading 4-byte marker

            parser.handler(buffer -> {

                if (currentState == RpcParseState.READING_MARKER) {
                    // 我们得到了4字节的记录标记
                    long recordMarkerRaw = buffer.getUnsignedInt(0); // 读取为无符号整数
                    isLastFragment = (recordMarkerRaw & 0x80000000L) != 0;
                    expectedFragmentLength = (int) (recordMarkerRaw & 0x7FFFFFFF); // 低31位是长度

                    System.out.println("Parsed Marker: last=" + isLastFragment + ", length=" + expectedFragmentLength);

                    if (expectedFragmentLength == 0) { // 可能是心跳或空片段
                        // 重置为读取下一个标记 (RecordParser 自动回到 fixed(4))
                        parser.fixedSizeMode(4);
                        currentState = RpcParseState.READING_MARKER;
                    } else {
                        parser.fixedSizeMode(expectedFragmentLength); // 切换到读取片段数据模式
                        currentState = RpcParseState.READING_FRAGMENT_DATA;
                    }

                } else if (currentState == RpcParseState.READING_FRAGMENT_DATA) {
                    // 我们得到了片段数据
                    System.out.println("Received fragment data of length: " + buffer.length());
                    messageFragments.add(buffer);

                    if (isLastFragment) {
                        handleRpcRequest(socket);
                    }
                    // 无论是不是最后一个片段，下一个都应该是记录标记
                    parser.fixedSizeMode(4); // 重置为读取下一个标记
                    currentState = RpcParseState.READING_MARKER;
                }
            });

            parser.exceptionHandler(Throwable::printStackTrace); // 处理解析器可能抛出的异常

            // 为每个连接的 socket 设置数据处理器
            socket.handler(parser);

            // 设置关闭处理器
            socket.closeHandler(v -> {
                log.info("客户端断开连接: " + socket.remoteAddress());
            });

            // 设置异常处理器
            socket.exceptionHandler(throwable -> {
                System.err.println("客户端 [" + socket.remoteAddress() + "] 发生错误: " + throwable.getMessage());
                socket.close(); // 发生错误时关闭连接
            });
        });

        // 启动服务器并监听端口
        server.listen(PORT, HOST, s -> {
            if (s.succeeded()) {
                log.info("Server started on host " + " and port " + s.result().actualPort());
            } else {
                System.err.println("Failed to start server: " + s.failed());

            }
        });



        // 也可以直接指定端口和主机，而不使用 NetServerOptions
        // server.listen(PORT, HOST, res -> { /* ... */ });
    }

    private void handleRpcRequest(NetSocket netSocket) {
        System.out.println("Processing complete message with " + messageFragments.size() + " fragments.");
        if (messageFragments.isEmpty()) {
            System.out.println("Received an empty RPC message.");
            // 处理空消息，如果协议允许
        } else {
            // 将所有片段合并成一个大的 Buffer
            Buffer fullMessage = Buffer.buffer();
            for (Buffer fragment : messageFragments) {
                fullMessage.appendBuffer(fragment);
            }
            System.out.println("Full message length: " + fullMessage.length());
            // 在这里反序列化和处理 fullMessage
            // e.g., MyRpcResponse response = XDR.decode(fullMessage, MyRpcResponse.class);
            String receivedData = fullMessage.toString("UTF-8");
            log.info("从客户端 [" + netSocket.remoteAddress() + "] 收到数据大小: " + receivedData.length());

//      log.info("Raw request buffer (" + fullMessage.length() + " bytes):");
//      // 简单的十六进制打印
//      for (int i = 0; i < fullMessage.length(); i++) {
//        System.out.printf("%02X ", fullMessage.getByte(i));
//        if ((i + 1) % 16 == 0 || i == fullMessage.length() - 1) {
//          System.out.println();
//        }
//      }
            //log.info("---- End of Raw request Buffer ----");

            // Parse RPC header
            //int recordMakerRaw = fullMessage.getInt(0);
            RpcHeader rpcHeader = new RpcHeader();
            int startOffset = rpcHeader.fromBuffer(fullMessage, 0);
            int programNumber = rpcHeader.getProgramNumber();
            int programVersion = rpcHeader.getProgramVersion();

            // Handle NFS requests
            if (programNumber == NFS_PROGRAM && programVersion == NFS_VERSION) {
                handleNFSRequest(fullMessage, netSocket);
            }
            // Handle NFS_ACL requests
            else if (programNumber == NFS_ACL_PROGRAM && programVersion == NFS_ACL_VERSION) {
                handleNFSACLRequest(fullMessage, netSocket);
            }
            else {
                log.error("Unsupported program: program={}, version={}", Optional.of(programNumber), Optional.of(programVersion));
            }

        }
        messageFragments.clear(); // 清空以便处理下一个消息
    }

    private void handleNFSRequest(Buffer buffer, NetSocket netSocket) {
        try {
            // Parse RPC header
            //int recordMakerRaw = buffer.getInt(0);
            RpcHeader rpcHeader = new RpcHeader();
            int startOffset = rpcHeader.fromBuffer(buffer, 0);
            int xid = rpcHeader.getXid();
            int programNumber = rpcHeader.getProgramNumber();
            int programVersion = rpcHeader.getProgramVersion();
            int procedureNumber = rpcHeader.getProcedureNumber();

            log.info("NFS Request - XID: 0x{}, Program: {}, Version: {}, Procedure: {}",
                    (Object) Integer.toHexString(xid), Optional.of(programNumber), Optional.of(programVersion), Optional.of(procedureNumber));

            // Verify this is an NFS request
            if (programNumber != NFS_PROGRAM || programVersion != NFS_VERSION) {
                log.error("Invalid NFS program number or version: program={}, version={}",
                        Optional.of(programNumber), Optional.of(programVersion));
                return;
            }

            Nfs3Procedure procedureNumberEnum = EnumUtil.fromCode(Nfs3Procedure.class, procedureNumber);

            byte[] xdrReplyBytes = null;
            switch (procedureNumberEnum) {
                case NFSPROC_NULL:
                    xdrReplyBytes = createNfsNullReply(xid);
                    break;
                case NFSPROC_GETATTR:
                    xdrReplyBytes = createNfsGetAttrReply(xid, buffer, startOffset);
                    break;
                case NFSPROC_SETATTR:
                    xdrReplyBytes = createNfsSetAttrReply(xid, buffer, startOffset);
                    break;
                case NFSPROC_LOOKUP:
                    xdrReplyBytes = createNfsLookupReply(xid, buffer, startOffset);
                    break;
                case NFSPROC_ACCESS:
                    xdrReplyBytes = createNfsAccessReply(xid, buffer, startOffset);
                    break;
//                case NFSPROC_READLINK:
//                    xdrReplyBytes = createNfsReadLinkReply(xid, buffer, startOffset);
//                    break;
                case NFSPROC_READ:
                    xdrReplyBytes = createNfsReadReply(xid, buffer, startOffset);
                    break;
                case NFSPROC_WRITE:
                    xdrReplyBytes = createNfsWriteReply(xid, buffer, startOffset);
                    break;
                case NFSPROC_CREATE:
                    xdrReplyBytes = createNfsCreateReply(xid, buffer, startOffset);
                    break;
                case NFSPROC_MKDIR:
                    xdrReplyBytes = createNfsMkdirReply(xid, buffer, startOffset);
                    break;
//                case NFSPROC_SYMLINK:
//                    xdrReplyBytes = createNfsSymlinkReply(xid, buffer, startOffset);
//                    break;
//                case NFSPROC_MKNOD:
//                    xdrReplyBytes = createNfsMknodReply(xid, buffer, startOffset);
//                    break;
               case NFSPROC_REMOVE:
                   xdrReplyBytes = createNfsRemoveReply(xid, buffer, startOffset);
                   break;
               case NFSPROC_RMDIR:
                   xdrReplyBytes = createNfsRmdirReply(xid, buffer, startOffset);
                   break;
//                case NFSPROC_RENAME:
//                    xdrReplyBytes = createNfsRenameReply(xid, buffer, startOffset);
//                    break;
//                case NFSPROC_LINK:
//                    xdrReplyBytes = createNfsLinkReply(xid, buffer, startOffset);
//                    break;
//                case NFSPROC_READDIR:
//                    xdrReplyBytes = createNfsReadDirReply(xid, buffer, startOffset);
//                    break;
                case NFSPROC_READDIRPLUS:
                    xdrReplyBytes = createNfsReadDirPlusReply(xid, buffer, startOffset);
                    break;
                case NFSPROC_FSSTAT:
                    xdrReplyBytes = createNfsFSStatReply(xid, buffer, startOffset);
                    break;
                case NFSPROC_FSINFO:
                    xdrReplyBytes = createNfsFSInfoReply(xid);
                    break;
                case NFSPROC_PATHCONF:
                    xdrReplyBytes = createNfsPathConfReply(xid, buffer, startOffset);
                    break;
                case NFSPROC_COMMIT:
                    xdrReplyBytes = createNfsCommitReply(xid, buffer, startOffset);
                    break;
                default:
                    log.error("Unsupported NFS procedure: {}", Optional.of(procedureNumber));
                    return;
            }

            if (xdrReplyBytes != null) {
                log.info("Sending NFS response - XID: 0x{}, Size: {} bytes",
                        Optional.of(Integer.toHexString(xid)), Optional.of(xdrReplyBytes.length));

                Buffer replyBuffer = Buffer.buffer(xdrReplyBytes);

                netSocket.write(replyBuffer);
            }
        } catch (Exception e) {
            log.error("Error processing NFS request", e);
        }
    }

    private void handleNFSACLRequest(Buffer buffer, NetSocket netSocket) {
        try {
            // Parse RPC header
            //int recordMakerRaw = buffer.getInt(0);
            RpcHeader rpcHeader = new RpcHeader();
            int startOffset = rpcHeader.fromBuffer(buffer, 0);

            int xid = rpcHeader.getXid();
            int procedureNumber = rpcHeader.getProcedureNumber();

            log.info("NFS_ACL Request - XID: 0x{}, Procedure: {}",
                    Optional.of(Integer.toHexString(xid)), Optional.of(procedureNumber));

            byte[] xdrReplyBytes = null;
            switch (procedureNumber) {
                case NFSPROC_ACL_NULL:
                    xdrReplyBytes = createNfsNullReply(xid);
                    break;
                case NFSPROC_ACL_GETACL:
                    xdrReplyBytes = createNfsACLGetACLReply(xid, buffer, startOffset);
                    break;
                case NFSPROC_ACL_SETACL:
                    xdrReplyBytes = createNfsACLSetACLReply(xid, buffer, startOffset);
                    break;
                default:
                    log.error("Unsupported NFS_ACL procedure: {}", Optional.of(procedureNumber));
                    return;
            }

            if (xdrReplyBytes != null) {
                log.info("Sending NFS_ACL response - XID: 0x{}, Size: {} bytes",
                        Optional.of(Integer.toHexString(xid)), Optional.of(xdrReplyBytes.length));
                netSocket.write(Buffer.buffer(xdrReplyBytes));
            }
        } catch (Exception e) {
            log.error("Error processing NFS_ACL request", e);
        }
    }

    // RPC Constants (values are in decimal for Java int literals)
    private static final int MSG_TYPE_REPLY = 1;          // 0x00000001
    private static final int REPLY_STAT_MSG_ACCEPTED = 0; // 0x00000000
    private static final int VERF_FLAVOR_AUTH_NONE = 0;   // 0x00000000
    private static final int VERF_LENGTH_ZERO = 0;        // 0x00000000
    private static final int ACCEPT_STAT_SUCCESS = 0;     // 0x00000000

    public static byte[] createNfsNullReply(int requestXid) {
        // --- Calculate RPC Message Body Length ---
        // XID (4 bytes)
        // Message Type (4 bytes)
        // Reply Status (4 bytes)
        // Verifier Flavor (4 bytes)
        // Verifier Length (4 bytes)
        // Acceptance Status (4 bytes)
        // Total = 6 * 4 = 24 bytes
        final int rpcMessageBodyLength = 24;

        // --- Create ByteBuffer for the RPC Message Body ---
        // We will fill this first, then prepend the record mark.
        ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
        rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN); // XDR is Big Endian

        // 1. XID (Transaction Identifier) - from request
        rpcBodyBuffer.putInt(requestXid);

        // 2. Message Type (mtype)
        rpcBodyBuffer.putInt(MSG_TYPE_REPLY);

        // 3. Reply Body (reply_body)
        //    3.1. Reply Status (stat of union switch (msg_type mtype))
        rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);

        //    3.2. Accepted Reply (areply)
        //        3.2.1. Verifier (verf - opaque_auth structure)
        rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE); // Flavor
        rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);      // Length of body (0 for AUTH_NONE)
        // Body is empty

        //        3.2.2. Acceptance Status (stat of union switch (accept_stat stat))
        rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

        //        3.2.3. Results (for NFSPROC3_NULL, this is void, so no data)

        // --- Construct Record Marking ---
        // Highest bit set (0x80000000) ORed with the length of the RPC message body.
        // In Java, an int is 32-bit.
        int recordMarkValue = 0x80000000 | rpcMessageBodyLength;

        // --- Create ByteBuffer for the Full XDR Response ---
        // Record Mark (4 bytes) + RPC Message Body (rpcMessageBodyLength bytes)
        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);

        // Put the record mark
        fullResponseBuffer.putInt(recordMarkValue);
        // Put the RPC message body (which is already in rpcBodyBuffer)
        fullResponseBuffer.put(rpcBodyBuffer.array()); // .array() gets the underlying byte array

        // Return the complete byte array
        return fullResponseBuffer.array();
    }

    private byte[] createNfsACLGetACLReply(int xid, Buffer request, int startOffset) throws IOException {
        // Parse file handle from request
        int fhandleLength = request.getInt(startOffset);
        byte[] fhandle = request.slice(startOffset + 4, startOffset + 4 + fhandleLength).getBytes();

        // Create reply
        final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
        ByteBuffer rpcBodyBuffer = RpcUtil.createAcceptedSuccessReplyHeaderBuffer(xid);

        PostOpAttr postOpAttr = PostOpAttr.builder().attributesFollow(0).build();
        GETACL3resfail getacl3resfail = GETACL3resfail.builder().objAttributes(postOpAttr).build();

        GETACL3res getacl3res = GETACL3res.createFailure(NfsStat3.NFS3ERR_NOTSUPP, getacl3resfail);

        int rpcNfsLength = getacl3res.getSerializedSize();
        ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
        rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

        getacl3res.serialize(rpcNfsBuffer);

        // Record marking
        int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);

        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcHeaderLength + rpcNfsLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
        fullResponseBuffer.putInt(recordMarkValue);
        fullResponseBuffer.put(rpcBodyBuffer.array());
        fullResponseBuffer.put(rpcNfsBuffer.array());

        return fullResponseBuffer.array();
    }

    private byte[] createNfsACLSetACLReply(int xid, Buffer request, int startOffset) throws IOException {
        // Create reply
        final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
        ByteBuffer rpcBodyBuffer = RpcUtil.createAcceptedSuccessReplyHeaderBuffer(xid);

        PostOpAttr postOpAttr = PostOpAttr.builder().attributesFollow(0).build();
        GETACL3resfail getacl3resfail = GETACL3resfail.builder().objAttributes(postOpAttr).build();

        GETACL3res getacl3res = GETACL3res.createFailure(NfsStat3.NFS3ERR_NOTSUPP, getacl3resfail);

        int rpcNfsLength = getacl3res.getSerializedSize();
        ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
        rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

        getacl3res.serialize(rpcNfsBuffer);

        // Record marking
        int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);

        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcHeaderLength + rpcNfsLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
        fullResponseBuffer.putInt(recordMarkValue);
        fullResponseBuffer.put(rpcBodyBuffer.array());
        fullResponseBuffer.put(rpcNfsBuffer.array());

        return fullResponseBuffer.array();
    }

    private byte[] createNfsGetAttrReply(int xid, Buffer request, int startOffset) throws IOException {
        // Parse file handle from request
        int fhandleLength = request.getInt(startOffset);
        byte[] fhandle = request.slice(startOffset + 4, startOffset + 4 + fhandleLength).getBytes();

        // Create reply
        final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
        ByteBuffer rpcHeaderBuffer = RpcUtil.createAcceptedSuccessReplyHeaderBuffer(xid);

        NFSFileHandle nfsFileHandle = NFSFileHandle.fromHexArray(fhandle);

        long inodeId = nfsFileHandle.getInodeId(); // fileHandleToFileId.getOrDefault(new ByteArrayKeyWrapper(fhandle), Long.valueOf(0x0000000002000002L));
        String targetVnodeId = MetaKeyUtils.getTargetVnodeId(BUCK_NAME);
        Optional<Inode> inodeOptional = MyRocksDB.getINodeMetaData(targetVnodeId, BUCK_NAME, inodeId);

        GETATTR3res getattr3res = null;
        if (inodeOptional.isPresent()) {
            Inode inode = inodeOptional.get();
            FAttr3 objAttributes = inode.toFAttr3();
            objAttributes.setFsidMajor(0);
            objAttributes.setFsidMinor((int)nfsFileHandle.getFsid());
            
            GETATTR3resok getattr3resok = GETATTR3resok.builder()
                .objAttributes(objAttributes)
                .build();

            getattr3res = GETATTR3res.createOk(getattr3resok);
        } else if (inodeId == 1L) {
            GETATTR3resok getattr3resok = GETATTR3resok.builder()
                .objAttributes(rootDirAttributes)
                .build();

            getattr3res = GETATTR3res.createOk(getattr3resok);
        } else {
            GETATTR3resfail getattr3resfail = GETATTR3resfail.builder().build();

            getattr3res = GETATTR3res.createFail(NfsStat3.NFS3ERR_SERVERFAULT, getattr3resfail);
        }

        int rpcNfsLength = getattr3res.getSerializedSize();
        ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
        rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

        getattr3res.serialize(rpcNfsBuffer);

        // Record marking
        int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);

        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcHeaderLength + rpcNfsLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
        fullResponseBuffer.putInt(recordMarkValue);
        fullResponseBuffer.put(rpcHeaderBuffer.array());
        fullResponseBuffer.put(rpcNfsBuffer.array());

        return fullResponseBuffer.array();
    }



    private byte[] createNfsSetAttrReply(int xid, Buffer request, int startOffset) throws IOException, IllegalAccessException {
        // Parse file handle from request
        //int fhandleLength = request.getInt(startOffset);
        //byte[] fhandle = request.slice(startOffset + 4, startOffset + 4 + fhandleLength).getBytes();
        byte[] requestByteData = request.slice(startOffset, request.length()).getBytes();
        SETATTR3args setattr3args = SETATTR3args.deserialize(requestByteData);

        // Create reply
        final int rpcMessageBodyLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
        ByteBuffer rpcBodyBuffer = RpcUtil.createAcceptedSuccessReplyHeaderBuffer(xid);

        // NFS SETATTR reply
        // Structure:
        // status (4 bytes)
        // pre_op_attr present flag (4 bytes)
        // post_op_attr present flag (4 bytes)
        NFSFileHandle nfsFileHandle = NFSFileHandle.fromHexArray(setattr3args.getObject().getFileHandle());

        log.info("fsid {}, inodeId: {}", nfsFileHandle.getFsid(), nfsFileHandle.getInodeId());

        SETATTR3res setattr3res = null;

        Optional<Inode> inodeOptional = MyRocksDB.getINodeMetaData(
                MetaKeyUtils.getTargetVnodeId(BUCK_NAME),
                BUCK_NAME,
                nfsFileHandle.getInodeId());

        if (inodeOptional.isPresent()) {
            Inode inode = inodeOptional.get();

            SetAttr3 newAttributes = setattr3args.getNewAttributes();
            int modeSetIt = newAttributes.getMode().getSetIt();
            if (modeSetIt != 0) {
                int mode = newAttributes.getMode().getMode();
                inode.setMode(mode);
            }
            int uidSetIt = newAttributes.getUid().getSetIt();
            if (uidSetIt != 0) {
                int uid = newAttributes.getUid().getUid();
                inode.setUid(uid);
            }
            int gidSetIt = newAttributes.getGid().getSetIt();
            if (gidSetIt != 0) {
                int gid = newAttributes.getGid().getGid();
                inode.setGid(gid);
            }
            int sizeSetIt = newAttributes.getSize().getSetIt();
            if (sizeSetIt != 0) {
                long size = newAttributes.getSize().getSize();
                inode.setSize(size);
            }
            int atimeSetToServerTime = newAttributes.getAtime();
            int mtimeSetToServerTIme = newAttributes.getMtime();
            long currentTimeMillis = System.currentTimeMillis();
            int seconds = (int)(currentTimeMillis / 1000);
            int nseconds = (int)((currentTimeMillis % 1000) * 1_000_000);
            if (atimeSetToServerTime != 0) {
                inode.setAtime(atimeSetToServerTime);
                inode.setAtimensec(nseconds);
            }
            if (mtimeSetToServerTIme != 0) {
                inode.setMtime(seconds);
                inode.setMtimensec(nseconds);
            }

            List<Inode.InodeData> list = inode.getInodeData();
            if (list.isEmpty() && inode.getSize() > 0) {
                createHole(list, Inode.InodeData.newHoleFile(newAttributes.getSize().getSize()), inode);
            }

            MyRocksDB.saveINodeMetaData(MetaKeyUtils.getTargetVnodeId(BUCK_NAME), inode);

            FAttr3 attributes = inode.toFAttr3();

            PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
            PostOpAttr after = PostOpAttr.builder()
                    .attributesFollow(1)
                    .attributes(attributes)
                    .build();
            WccData objWcc = WccData.builder()
                    .before(before)
                    .after(after)
                    .build();
            SETATTR3resok setattr3resok = SETATTR3resok.builder()
                    .objWcc(objWcc)
                    .build();

            setattr3res = SETATTR3res.createSuccess(setattr3resok);

        } else {
            WccData objWcc = WccData.builder().before(PreOpAttr.builder().build()).after(PostOpAttr.builder().build()).build();
            SETATTR3resfail setattr3resfail = SETATTR3resfail.builder().objWcc(objWcc).build();
            setattr3res = SETATTR3res.createFailure(NfsStat3.NFS3ERR_NOENT, setattr3resfail);
        }

        int rpcNfsLength = setattr3res.getSerializedSize();
        ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
        rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

        setattr3res.serialize(rpcNfsBuffer);

        // Record marking
        int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
        fullResponseBuffer.putInt(recordMarkValue);
        fullResponseBuffer.put(rpcBodyBuffer.array());
        fullResponseBuffer.put(rpcNfsBuffer.array());

        return fullResponseBuffer.array();
    }

    private byte[] createNfsLookupReply(int xid, Buffer request, int startOffset) throws IOException {
        // Parse directory file handle and name from request
        int dirFhandleLength = request.getInt(startOffset);
        byte[] dirFhandle = request.slice(startOffset + 4, startOffset + 4 + dirFhandleLength).getBytes();
        int nameLength = request.getInt(startOffset + 4 + dirFhandleLength);
        String name = request.slice(startOffset + 4 + dirFhandleLength + 4,
                startOffset + 4 + dirFhandleLength + 4 + nameLength).toString("UTF-8");

        log.info("LOOKUP request - directory handle length: {}, name: {}", Optional.of(dirFhandleLength), name);

        // Create reply
        int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
        ByteBuffer rpcHeaderBuffer = RpcUtil.createAcceptedSuccessReplyHeaderBuffer(xid);

        // Generate a unique file handle based on the name
        NFSFileHandle nfsFileHandle = NFSFileHandle.fromHexArray(dirFhandle);
        long parentInodeId = nfsFileHandle.getInodeId();
        String targetVnodeId = MetaKeyUtils.getTargetVnodeId(BUCK_NAME);
        Optional<Inode> parentInodeOptional = MyRocksDB.getINodeMetaData(targetVnodeId, BUCK_NAME, parentInodeId);
        byte[] fileHandle = new byte[0];
        long childInode = 0;
        if (parentInodeOptional.isPresent()) {
            Inode parentInode = parentInodeOptional.get();
            String parentPath = parentInode.getObjName();
            String fullPath;
            if (parentPath.endsWith("/")) {
                fullPath = parentPath + name;
            } else {
                fullPath = parentPath + "/" + name;
            }
            Optional<VersionIndexMetadata> vOptional = MyRocksDB.getIndexMetaData(targetVnodeId, BUCK_NAME, fullPath);
            if (vOptional.isPresent()) {
                VersionIndexMetadata versionIndexMetadata = vOptional.get();
                long inodeId = versionIndexMetadata.getInode();
                NFSFileHandle childNfsFileHandle = NFSFileHandle.builder()
                        .fsid(nfsFileHandle.getFsid())
                        .inodeId(inodeId)
                        .build();
                fileHandle = childNfsFileHandle.toHexArray();
            }
        }

        int fileHandleLength = fileHandle.length;

        LOOKUP3res lookup3res = null;
        if (fileHandleLength > 0) {
            FAttr3 fAttr3 = inodeIdToFAttr3.getOrDefault(childInode, null);
            PostOpAttr objAttributes = PostOpAttr.builder()
                    .attributesFollow(fAttr3 != null ? 1 : 0)
                    .attributes(fAttr3)
                    .build();

            PostOpAttr dirAttributes = PostOpAttr.builder()
                    .attributesFollow(0)
                    .build();

            LOOKUP3resok lookup3resok = LOOKUP3resok.builder()
                    .objHandlerLength(fileHandleLength)
                    .objectHandleData(fileHandle)
                    .objAttributes(objAttributes)
                    .dirAttributes(dirAttributes)
                    .build();

            lookup3res = LOOKUP3res.createOk(lookup3resok);
        } else {

            FAttr3 dirAttr3 = fileHandleToFAttr3.getOrDefault(new ByteArrayKeyWrapper(dirFhandle), null);
            PostOpAttr dirAttributes = PostOpAttr.builder()
                    .attributesFollow(dirAttr3 != null ? 1 : 0)
                    .attributes(dirAttr3)
                    .build();

            LOOKUP3resfail lookup3resfail = LOOKUP3resfail.builder()
                    .dirAttributes(dirAttributes)
                    .build();

            lookup3res = LOOKUP3res.createFail(NfsStat3.NFS3ERR_NOENT, lookup3resfail);
        }

        int rpcNfsLength = lookup3res.getSerializedSize();

        ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
        rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

        lookup3res.serialize(rpcNfsBuffer);

        // Record marking
        int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);

        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcHeaderLength + rpcNfsLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
        fullResponseBuffer.putInt(recordMarkValue);
        fullResponseBuffer.put(rpcHeaderBuffer.array());
        fullResponseBuffer.put(rpcNfsBuffer.array());

        return fullResponseBuffer.array();
    }

    private byte[] createNfsAccessReply(int xid, Buffer request, int startOffset) throws IOException {
        // Parse file handle from request
        int fhandleLength = request.getInt(startOffset);
        byte[] fhandle = request.slice(startOffset + 4, startOffset + 4 + fhandleLength).getBytes();

        // Parse access request flags
        int accessRequest = request.getInt(startOffset + 4 + fhandleLength);
        log.info("ACCESS request - handle length: {}, access request: 0x{}",
                Optional.of(fhandleLength), Integer.toHexString(accessRequest));

        // Create reply
        int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
        ByteBuffer rpcHeaderBuffer = RpcUtil.createAcceptedSuccessReplyHeaderBuffer(xid);

        // NFS ACCESS reply
        // Determine file type from handle
        NFSFileHandle nfsFileHandle = NFSFileHandle.fromHexArray(fhandle);
        long inodeId = nfsFileHandle.getInodeId();
        
        Optional<Inode> inodeOptional = MyRocksDB.getINodeMetaData(
                MetaKeyUtils.getTargetVnodeId(BUCK_NAME),
                BUCK_NAME,
                inodeId);

        ACCESS3res access3res = null;

        if (inodeOptional.isPresent()) {
            Inode inode = inodeOptional.get();
            String filename = inode.getObjName();
            FAttr3 fAttr3 = inode.toFAttr3();
            // Determine file type from inode mode
            int mode = fAttr3.getMode();
            int fileType = fAttr3.getType(); // Extract file type bits
            log.info("ACCESS - inodeId: {}, filename: {}, mode: {}, fileTypeFromMode: {}",
                    Optional.of(inodeId), filename, Optional.of(mode), Optional.of(fileType));

            // ACCESS flags
            // ACCESS_READ     = 0x0001
            // ACCESS_LOOKUP   = 0x0002
            // ACCESS_MODIFY   = 0x0004
            // ACCESS_EXTEND   = 0x0008
            // ACCESS_DELETE   = 0x0010
            // ACCESS_EXECUTE  = 0x0020
            int accessFlags = 0;

            if (fileType == 2) { // Directory
                // For directories, allow READ, LOOKUP, MODIFY, EXTEND, DELETE
                accessFlags = 0x0001 | // ACCESS_READ
                        0x0002 | // ACCESS_LOOKUP
                        0x0004 | // ACCESS_MODIFY
                        0x0008 | // ACCESS_EXTEND
                        0x0010;  // ACCESS_DELETE
            } else { // Regular file
                // For regular files, allow READ, MODIFY, EXTEND, DELETE, EXECUTE
                accessFlags = 0x0001 | // ACCESS_READ
                        0x0004 | // ACCESS_MODIFY
                        0x0008 | // ACCESS_EXTEND
                        0x0010 | // ACCESS_DELETE
                        0x0020;  // ACCESS_EXECUTE
            }

            // Only return the flags that were requested
            accessFlags &= accessRequest;

            log.info("ACCESS response - file type: {}, granted access: 0x{}",
                    Optional.of(fileType), Integer.toHexString(accessFlags));


            FAttr3 dirAttr3 = fileHandleToFAttr3.getOrDefault(new ByteArrayKeyWrapper(fhandle), null);
            PostOpAttr objAttributes = PostOpAttr.builder()
                    .attributesFollow(dirAttr3 != null ? 1 : 0)
                    .attributes(dirAttr3)
                    .build();

            ACCESS3resok access3resok = ACCESS3resok.builder()
                    .objAttributes(objAttributes)
                    .accessFlags(accessFlags)
                    .build();

            access3res = ACCESS3res.createOk(access3resok);
        } else if (inodeId == 1) {
            // Root directory always exists
            int accessFlags = 0x0001 | // ACCESS_READ
                    0x0002 | // ACCESS_LOOKUP
                    0x0004 | // ACCESS_MODIFY
                    0x0008 | // ACCESS_EXTEND
                    0x0010;  // ACCESS_DELETE

            FAttr3 dirAttr3 = fileHandleToFAttr3.getOrDefault(new ByteArrayKeyWrapper(fhandle), null);
            PostOpAttr objAttributes = PostOpAttr.builder()
                    .attributesFollow(dirAttr3 != null ? 1 : 0)
                    .attributes(dirAttr3)
                    .build();

            ACCESS3resok access3resok = ACCESS3resok.builder()
                    .objAttributes(objAttributes)
                    .accessFlags(accessFlags)
                    .build();

            access3res = ACCESS3res.createOk(access3resok);
            
        } else {
            ACCESS3resfail access3resfail = ACCESS3resfail.builder()
                    .dirAttributes(PostOpAttr.builder().build())
                    .build();
            access3res = ACCESS3res.createFail(NfsStat3.NFS3ERR_NOENT, access3resfail);
            log.info("ACCESS - inodeId: {} not found", Optional.of(inodeId));
        }

        int rpcNfsLength = access3res.getSerializedSize();
        ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
        rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

        access3res.serialize(rpcNfsBuffer);

        // Record marking
        int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);

        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcHeaderLength + rpcNfsLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
        fullResponseBuffer.putInt(recordMarkValue);
        fullResponseBuffer.put(rpcHeaderBuffer.array());
        fullResponseBuffer.put(rpcNfsBuffer.array());

        return fullResponseBuffer.array();
    }

     /**
        * @param dataList 字节数组列表
        * @return 合并后的单个字节数组
     */
    public static byte[] mergeWithByteBuffer(List<byte[]> dataList) {
        // 1. 计算总长度（与方法一相同）
        int totalLength = 0;
        for (byte[] array : dataList) {
            totalLength += array.length;
        }

        // 2. 分配一个具有总长度的 ByteBuffer
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);

        // 3. 将每个数组放入 buffer 中
        for (byte[] array : dataList) {
            buffer.put(array);
        }

        // 4. 从 buffer 中获取底层的完整数组
        return buffer.array();
    }

    public static List<Inode.InodeData> partialRead(ChunkFile chunkFile, long readOffset, long readSize) {
        List<Inode.InodeData> result = new ArrayList<>();
        long curOffset = 0L;
        long readEnd = readOffset + readSize;

        long calOffset = 0;
        for (Inode.InodeData cur : chunkFile.getChunkList()) {
            //cur.offset = calOffset;
            //calOffset += cur.size;

            long curEnd = curOffset + cur.size;

            // 当前块完全在读取区间左侧，跳过
            if (curEnd <= readOffset) {
                curOffset = curEnd;
                continue;
            }

            // 当前块完全在读取区间右侧，结束
            if (curOffset >= readEnd) {
                break;
            }
//         [2169661,    3145728]
//            [3076096, 3145728]
            // 计算重叠区间
            long overlapStart = Math.max(curOffset, readOffset);
            long overlapEnd = Math.min(curEnd, readEnd);

            if (overlapStart < overlapEnd) {
                // 构造只包含重叠部分的新 InodeData
                Inode.InodeData part = new Inode.InodeData(cur);
                part.offset = overlapStart - curOffset;
                part.size = overlapEnd - overlapStart;
                result.add(part);
            }

            curOffset = curEnd;
        }

        return result;
    }

    private byte[] createNfsReadReply(int xid, Buffer request, int startOffset) throws IOException {
        // Parse file handle, offset, and count from request
        int fhandleLength = request.getInt(startOffset);
        byte[] fhandle = request.slice(startOffset + 4, startOffset + 4 + fhandleLength).getBytes();
        long readOffset = request.getLong(startOffset + 4 + fhandleLength);
        int count = request.getInt(startOffset + 4 + fhandleLength + 8);

        // Create reply
        final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
        ByteBuffer rpcBodyBuffer = RpcUtil.createAcceptedSuccessReplyHeaderBuffer(xid);

        NFSFileHandle nfsFileHandle = NFSFileHandle.fromHexArray(fhandle);
        long inodeId = nfsFileHandle.getInodeId();

        Optional<Inode> inodeOptional = MyRocksDB.getINodeMetaData(MetaKeyUtils.getTargetVnodeId(BUCK_NAME), BUCK_NAME, inodeId); 
        Optional<READ3res> read3resOptinal = Optional.empty();
        if (inodeOptional.isPresent()) {
            Inode inode = inodeOptional.get();
            FAttr3 fAttr3 = inode.toFAttr3();

            List<Inode.InodeData> list = inode.getInodeData();
            if (!list.isEmpty()) {
                Inode.InodeData last = list.get(list.size() - 1);
                String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(inode.getBucket(), last.fileName);
                ChunkFile chunkFile = MyRocksDB.getChunkFileMetaData(chunkKey).orElseThrow(() -> new RuntimeException("chunk file not found for chunkKey: " + chunkKey));

                List<Inode.InodeData> overlappingDataSegments = partialRead(chunkFile, readOffset, count);

                if (!overlappingDataSegments.isEmpty()) {
                    List<byte[]> allData = new ArrayList<>();
                    for (Inode.InodeData inodeData : overlappingDataSegments) {
                        if (!StringUtils.isBlank(inodeData.getFileName())) {
                            FileMetadata fileMetadata = MyRocksDB.getFileMetaData(inodeData.getFileName()).orElseThrow(() -> new RuntimeException("readChunk failed..."));
                            List<Long> offsets = fileMetadata.getOffset();
                            List<Long> lens = fileMetadata.getLen();

                            List<byte[]> dataList = readSegment(offsets, lens, inodeData.size);
                            byte[] tmpData = mergeWithByteBuffer(dataList);

                            // 1. 创建一个大小为 size 的新数组
                            byte[] resultData = new byte[(int) inodeData.size];
                            // 2. 使用 System.arraycopy() 进行复制
                            System.arraycopy(tmpData, (int) inodeData.offset, resultData, 0, (int) inodeData.size);

                            allData.add(resultData);
                        } else {
                            byte[] byteArray = new byte[(int)inodeData.size];
                            allData.add(byteArray);
                        }
                    }

                    byte[] data = mergeWithByteBuffer(allData);
                    int dataLength = data.length / 4 * 4 + 4;

                    PostOpAttr fileAttributes = PostOpAttr.builder()
                            .attributesFollow(fAttr3 != null ? 1 : 0)
                            .attributes(fAttr3)
                            .build();

                    READ3resok read3resok = READ3resok.builder()
                            .fileAttributes(fileAttributes)
                            .count(dataLength)
                            .eof(1)
                            .dataOfLength(dataLength)
                            .data(data)
                            .build();

                    read3resOptinal = Optional.of(READ3res.createOk(read3resok));
                } else {
                    PostOpAttr fileAttributes = PostOpAttr.builder().attributesFollow(0).build();
                    READ3resfail read3resfail = READ3resfail.builder().fileAttributes(fileAttributes).build();
                    read3resOptinal = Optional.of(READ3res.createFail(NfsStat3.NFS3ERR_IO, read3resfail));
                }

            } else {
                PostOpAttr fileAttributes = PostOpAttr.builder().attributesFollow(0).build();
                READ3resfail read3resfail = READ3resfail.builder().fileAttributes(fileAttributes).build();
                read3resOptinal = Optional.of(READ3res.createFail(NfsStat3.NFS3ERR_NOENT, read3resfail));
            }

        }else {
            PostOpAttr fileAttributes = PostOpAttr.builder().attributesFollow(0).build();
            READ3resfail read3resfail = READ3resfail.builder().fileAttributes(fileAttributes).build();
            read3resOptinal = Optional.of(READ3res.createFail(NfsStat3.NFS3ERR_NOENT, read3resfail));
        }

        READ3res read3res = read3resOptinal.get();
        int rpcNfsLength = read3res.getSerializedSize();
        ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
        rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);
        read3res.serialize(rpcNfsBuffer);

        // Record marking
        int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);

        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcHeaderLength + rpcNfsLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
        fullResponseBuffer.putInt(recordMarkValue);
        fullResponseBuffer.put(rpcBodyBuffer.array());
        fullResponseBuffer.put(rpcNfsBuffer.array());

        return fullResponseBuffer.array();
    }

    public static List<byte[]> readSegment(List<Long> offsets, List<Long> lens, long size) throws IOException {
        if (!Paths.get(MyRocksDB.FILE_DATA_DEVICE_PATH).toFile().exists()) {
            log.error("No such path exists...");
            throw new IOException("No such path exists...");
        }

        List<byte[]> result = new ArrayList<>();

        for (int i = 0; i < offsets.size(); i++) {
            long offset = offsets.get(i);
            long length = lens.get(i);

            try(FileChannel fileChannel = FileChannel.open(Paths.get(MyRocksDB.FILE_DATA_DEVICE_PATH), StandardOpenOption.READ)) {
                ByteBuffer buffer = ByteBuffer.allocate((int)length);
                fileChannel.read(buffer, offset);
                buffer.flip();
                byte[] data = new byte[buffer.remaining()];
                buffer.get(data);

                result.add(data);
            }
        }

        return result;
    }

    public static void createData(List<Inode.InodeData> list, Inode.InodeData data, Inode inode, byte[] dataToWrite) throws JsonProcessingException {
        // 把普通 inodeData 转换为 chunk
        String targetVnodeId = MetaKeyUtils.getTargetVnodeId(inode.getBucket());

        String chunkKey = ChunkFile.getChunkKey(targetVnodeId, data.fileName);
        Inode.InodeData chunk = ChunkFile.newChunk(data.fileName, data, inode);
        ChunkFile chunkFile = ChunkFile.builder().nodeId(inode.getNodeId()).bucket(inode.getBucket()).objName(inode.getObjName())
                .versionId(inode.getVersionId()).versionNum(MetaKeyUtils.getVersionNum()).size(data.size).chunkList(new ArrayList<>()).hasDeleteFiles(new LinkedList<>()).build();

        chunkFile.getChunkList().add(data);
        list.add(chunk);

        chunkFile.setSize(data.size);
        inode.setSize(data.size);

        MyRocksDB.saveINodeMetaData(targetVnodeId, inode);
        // 关联 - chunkKey - chunk - chunkFile
        MyRocksDB.saveChunkFileMetaData(chunkKey, chunkFile);

        String verisonKey = MetaKeyUtils.getVersionMetaDataKey(targetVnodeId, inode.getBucket(), inode.getObjName(), null);
        MyRocksDB.saveFileMetaData(data.fileName, verisonKey, dataToWrite, dataToWrite.length, true);
    }

    public static void createHole(List<Inode.InodeData> list, Inode.InodeData holeFile, Inode inode) throws JsonProcessingException {
        String holeFileName = MetaKeyUtils.getHoleFileName(inode.getBucket(), getRequestId());
        String chunkKey = ChunkFile.getChunkKey(holeFileName);
        Inode.InodeData chunk = ChunkFile.newChunk(holeFileName, holeFile, inode);
        ChunkFile chunkFile = ChunkFile.builder().nodeId(inode.getNodeId()).bucket(inode.getBucket()).objName(inode.getObjName())
                .versionId(inode.getVersionId()).versionNum(MetaKeyUtils.getVersionNum()).size(holeFile.size).chunkList(new ArrayList<>()).hasDeleteFiles(new LinkedList<>()).build();

        chunkFile.getChunkList().add(holeFile);
        list.add(chunk);

        chunkFile.setSize(holeFile.size);
        inode.setSize(holeFile.size);

        String targetVnodeId = MetaKeyUtils.getTargetVnodeId(inode.getBucket());
        MyRocksDB.saveINodeMetaData(targetVnodeId, inode);
        // 关联 - chunkKey - chunk - chunkFile
        MyRocksDB.saveChunkFileMetaData(chunkKey, chunkFile);
    }

    public static void appendData(List<Inode.InodeData> list, Inode.InodeData data, Inode inode, byte[] dataToWrite) throws JsonProcessingException {
        if (list.isEmpty()) {
            if (StringUtils.isBlank(data.fileName)) {
                createHole(list ,data, inode);
            } else {
                createData(list, data, inode, dataToWrite);
            }
        }
        else {
            Inode.InodeData last = list.get(list.size() - 1);
            String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(inode.getBucket(), last.fileName);
            ChunkFile chunkFile = MyRocksDB.getChunkFileMetaData(chunkKey).orElseThrow(() -> new RuntimeException("chunk file not found..."));
            chunkFile.getChunkList().add(data);
            MyRocksDB.saveChunkFileMetaData(chunkKey, chunkFile);

            String targetVnodeId = MetaKeyUtils.getTargetVnodeId(inode.getBucket());
            if (StringUtils.isNotBlank(data.fileName) && dataToWrite != null) {
                // String vnodeId = data.fetchInodeDataTargetVnodeId();
                // List<Long> link = Arrays.asList(((long) Long.parseLong(vnodeId)));
                // String s_uuid = "0001";
                // MyRocksDB.saveRedis(vnodeId, link, s_uuid);

                String verisonKey = MetaKeyUtils.getVersionMetaDataKey(targetVnodeId, inode.getBucket(), inode.getObjName(), null);
                MyRocksDB.saveFileMetaData(data.fileName, verisonKey, dataToWrite, dataToWrite.length, true);
            }

            last.setSize(last.getSize() + data.size);
            inode.setSize(inode.getSize() + data.size);
            chunkFile.setSize(chunkFile.getSize() + data.size);
            last.setChunkNum(chunkFile.getChunkList().size());

            MyRocksDB.saveINodeMetaData(targetVnodeId, inode);
            MyRocksDB.saveChunkFileMetaData(chunkKey, chunkFile);
        }
    }

    public static String calculateMD5(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] hash = digest.digest(data);
            return bytesToHex2(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not available", e);
        }
    }

    private byte[] createNfsWriteReply(int xid, Buffer request, int startOffset) throws IOException, URISyntaxException {
        // Parse file handle, offset, and data from request
        int fhandleLength = request.getInt(startOffset);
        byte[] fhandle = request.slice(startOffset + 4, startOffset + 4 + fhandleLength).getBytes();
        long reqOffset = request.getLong(startOffset + 4 + fhandleLength);
        int count = request.getInt(startOffset + 4 + fhandleLength + 8);
        int stable = request.getInt(startOffset + 4 + fhandleLength + 8 + 4);
        int dataOfLength = request.getInt(startOffset + 4 + fhandleLength + 8);
        byte[] dataToWrite = request.slice(startOffset + 4 + fhandleLength + 8 + 12,
                startOffset + 4 + fhandleLength + 8 + 12 + dataOfLength).getBytes();

        // Create reply
        final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
        ByteBuffer rpcBodyBuffer = RpcUtil.createAcceptedSuccessReplyHeaderBuffer(xid);

        NFSFileHandle nfsFileHandle = NFSFileHandle.fromHexArray(fhandle);
        long inodeId = nfsFileHandle.getInodeId();
        Optional<Inode> inodeOptional = MyRocksDB.getINodeMetaData(
                MetaKeyUtils.getTargetVnodeId(BUCK_NAME),
                BUCK_NAME,
                inodeId);

        WRITE3res write3res = null;
        if (inodeOptional.isPresent()) {
            Inode inode = inodeOptional.get();
            FAttr3 attributes = inode.toFAttr3();
            String object = inode.getObjName();

            String filename = MetaKeyUtils.getObjFileName(BUCK_NAME, object, getRequestId());

            Inode.InodeData inodeData = new Inode.InodeData();
            inodeData.offset = reqOffset;
            inodeData.size = dataOfLength;
            inodeData.fileName = filename;
            inodeData.etag = calculateMD5(dataToWrite);
            inodeData.storage = "dataa";

            List<Inode.InodeData> list = inode.getInodeData();
            long reqEnd = reqOffset + count;
            // 如果写入的位置大于文件的末端
            if (reqOffset >= inode.getSize()) {
                if (reqOffset > inode.getSize()) {
                    appendData(list, Inode.InodeData.newHoleFile(reqOffset - inode.getSize()), inode, dataToWrite);
                    appendData(list, inodeData, inode, dataToWrite);
                }
                // 恰好追加
                else {
                    appendData(list, inodeData, inode, dataToWrite);
                }
            }
            // 新加入的 inodeata 与 原来的数据有交集
            else {
                Inode.InodeData last = list.get(list.size() - 1);
                String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(inode.getBucket(), last.fileName);
                ChunkFile chunkFile = MyRocksDB.getChunkFileMetaData(chunkKey).orElseThrow(() -> new RuntimeException("chunk file not found..."));
                long updatedtotalSize = Inode.partialOverwrite3(chunkFile, reqOffset, inodeData);

                String targetVnodeId = MetaKeyUtils.getTargetVnodeId(inode.getBucket());
                String verisonKey = MetaKeyUtils.getVersionMetaDataKey(targetVnodeId, inode.getBucket(), inode.getObjName(), inode.getVersionId());
                MyRocksDB.saveFileMetaData(inodeData.fileName, verisonKey, dataToWrite, dataToWrite.length, true);

                chunkFile.setSize(chunkFile.getSize() + updatedtotalSize);
                inode.setSize(chunkFile.getSize() + updatedtotalSize);
                last.setChunkNum(chunkFile.getChunkList().size());
                last.setSize(chunkFile.getSize() + updatedtotalSize);

                MyRocksDB.saveINodeMetaData(targetVnodeId, inode);
                MyRocksDB.saveChunkFileMetaData(chunkKey, chunkFile);
            }

            FAttr3 attritbutes = inode.toFAttr3();

            PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
            PostOpAttr after = PostOpAttr.builder().attributesFollow(1).attributes(attritbutes).build();

            WccData fileWcc = WccData.builder().before(before).after(after).build();
            WRITE3resok write3resok = WRITE3resok.builder()
                    .fileWcc(fileWcc)
                    .count(count)
                    .committed(WRITE3resok.StableHow.FILE_SYNC)
                    .verifier(0L)
                    .build();

            write3res = WRITE3res.createOk(write3resok);
        } else {
            PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
            PostOpAttr after = PostOpAttr.builder().attributesFollow(0).build();
            WccData fileWcc = WccData.builder().before(before).after(after).build();
            WRITE3resfail failData = WRITE3resfail.builder().fileWcc(fileWcc).build();

            write3res = WRITE3res.createFail(NfsStat3.NFS3ERR_BADHANDLE, failData);
        }

        // NFS WRITE reply
        int rpcNfsLength = write3res.getSerializedSize();

        ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
        rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

        write3res.serialize(rpcNfsBuffer);

        // Record marking
        int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);

        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcHeaderLength + rpcNfsLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
        fullResponseBuffer.putInt(recordMarkValue);
        fullResponseBuffer.put(rpcBodyBuffer.array());
        fullResponseBuffer.put(rpcNfsBuffer.array());

        return fullResponseBuffer.array();
    }

    private byte[] createNfsCreateReply(int xid, Buffer request, int startOffset) throws IOException {
        int dirFhandleLength = request.getInt(startOffset);
        byte[] dirFhandle = request.slice(startOffset + 4, startOffset + 4 + dirFhandleLength).getBytes();
        int nameLength = request.getInt(startOffset + 4 + dirFhandleLength);
        nameLength = (nameLength + 4 - 1 ) / 4 * 4;
        String name = request.slice(startOffset + 4 + dirFhandleLength + 4,
                startOffset + 4 + dirFhandleLength + 4 + nameLength).toString("UTF-8").trim();
        int createMode = request.getInt(startOffset + 4 + dirFhandleLength + 4 + nameLength);
        long verifier = request.getLong(startOffset + 4 + dirFhandleLength + 4 + nameLength + 4);

        // Create reply
        final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
        ByteBuffer rpcHeaderBuffer = RpcUtil.createAcceptedSuccessReplyHeaderBuffer(xid);

        NFSFileHandle dirFileHandle = NFSFileHandle.fromHexArray(dirFhandle);
        Optional<Inode> parentInodeOptional = MyRocksDB.getINodeMetaData(
                MetaKeyUtils.getTargetVnodeId(BUCK_NAME),
                BUCK_NAME,
                dirFileHandle.getInodeId());

        // NFS CREATE reply
        // Structure:
        // status (4 bytes)
        // file handle (32 bytes)
        // post_op_attr present flag (4 bytes)
        // wcc_data
        //   pre_op_attr present flag (4 bytes)
        //   post_op_attr present flag (4 bytes)

        // Object attributes
        // Determine file type based on name

        int permission = createMode; // Extract permission bits (lower 9 bits)
        int mode = Inode.Mode.S_IFREG.getCode() | permission;

        CREATE3res create3res = null;
        try {
            String dir = "";
            if (parentInodeOptional.isPresent()) {
                Inode parentInode = parentInodeOptional.get();
                dir = parentInode.getObjName();
            }

            if (dirFileHandle.getInodeId() == 1L) {
                dir = "/";
            }
            if (dir.length() > 0) {
                String bucket = BUCK_NAME;
                String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);
                String object = PathUtils.combine(dir, name);
                
                Inode inode = MyRocksDB.saveIndexMetaAndInodeData(targetVnodeId, bucket, object, 0, "application/octet-stream", mode).orElseThrow(() -> new RuntimeException("no such inode.."));

                byte[] fileHandle = NFSFileHandle.builder().inodeId(inode.getNodeId()).fsid(dirFileHandle.getFsid()).build().toHexArray();

                int fileHandleLength = fileHandle.length;
                    
                NfsFileHandle3 nfsFileHandle = NfsFileHandle3.builder()
                .handleOfLength(fileHandleLength)
                .fileHandle(fileHandle)
                .build();

                PostOpFileHandle3 obj = PostOpFileHandle3.builder()
                        .handleFollows(1)
                        .nfsFileHandle(nfsFileHandle)
                        .build();

                FAttr3 attributes = inode.toFAttr3();
                attributes.setFsidMajor(0);
                attributes.setFsidMinor((int) dirFileHandle.getFsid());
                PostOpAttr ojbAttributes = PostOpAttr.builder().attributesFollow(1).attributes(attributes).build();

                PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
                PostOpAttr after = PostOpAttr.builder().attributesFollow(0).build();
                WccData dirWcc = WccData.builder().before(before).after(after).build();

                CREATE3resok create3resok = CREATE3resok.builder()
                        .obj(obj)
                        .ojbAttributes(ojbAttributes)
                        .dirWcc(dirWcc)
                        .build();

                create3res = CREATE3res.createSuccess(create3resok);
            } else {
                PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
                PostOpAttr after = PostOpAttr.builder().attributesFollow(0).build();
                CREATE3resfail create3resfail = CREATE3resfail.builder()
                        .dirWcc(WccData.builder().before(before).after(after).build())
                        .build();
                create3res = CREATE3res.createFailure(NfsStat3.NFS3ERR_IO, create3resfail);
            }
        }catch (Exception e) {
            log.error("operate db fail..");
            PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
            PostOpAttr after = PostOpAttr.builder().attributesFollow(0).build();
            CREATE3resfail create3resfail = CREATE3resfail.builder()
                    .dirWcc(WccData.builder().before(before).after(after).build())
                    .build();
            create3res = CREATE3res.createFailure(NfsStat3.NFS3ERR_IO, create3resfail);
        }

        int rpcNfsLength = create3res.getSerializedSize();
        ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
        rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

        create3res.serialize(rpcNfsBuffer);

        // Record marking
        int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);

        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcHeaderLength + rpcNfsLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
        fullResponseBuffer.putInt(recordMarkValue);
        fullResponseBuffer.put(rpcHeaderBuffer.array());
        fullResponseBuffer.put(rpcNfsBuffer.array());

        return fullResponseBuffer.array();
    }

    private byte[] createNfsMkdirReply(int xid, Buffer request, int startOffset) throws IOException {
        MKDIR3args mkdir3args = new MKDIR3args();
        mkdir3args.deserialize(request, startOffset);
        // Create reply
        final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
        ByteBuffer rpcHeaderBuffer = RpcUtil.createAcceptedSuccessReplyHeaderBuffer(xid);
        // NFS MKDIR reply
        // Structure:
        // status (4 bytes)
        // file handle (32 bytes)
        // post_op_attr present flag (4 bytes)
        // wcc_data
        //   pre_op_attr present flag (4 bytes)
        //   post_op_attr present flag (4 bytes)
        byte[] dirHandle = mkdir3args.where.dir.fileHandle;
        NFSFileHandle dirFileHandle = NFSFileHandle.fromHexArray(dirHandle);
        MKDIR3res mkdir3res = null;

        int permission = mkdir3args.attributes.mode.setIt > 0 ? mkdir3args.attributes.mode.mode : 0755; // Extract permission bits (lower 9 bits)
        int mode = Inode.Mode.S_IFDIR.getCode() | permission;

        Optional<Inode> parentInodeOptional = MyRocksDB.getINodeMetaData(
            MetaKeyUtils.getTargetVnodeId(BUCK_NAME),
            BUCK_NAME,
            dirFileHandle.getInodeId());

        FAttr3 dirAttr = rootDirAttributes;
        try {
            String dir = "";
            if (parentInodeOptional.isPresent()) {
                Inode parentInode = parentInodeOptional.get();
                dir = parentInode.getObjName();
                dirAttr = parentInode.toFAttr3();
            }

            if (dirFileHandle.getInodeId() == 1L) {
                dir = "/";
            }
            if (dir.length() > 0) {
                String bucket = BUCK_NAME;
                String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);
                String object = PathUtils.combine(dir, new String(mkdir3args.where.filename, StandardCharsets.UTF_8));
                Inode inode = MyRocksDB.saveIndexMetaAndInodeData(targetVnodeId, bucket, object, 4096, "application/octet-stream", mode).orElseThrow(() -> new RuntimeException("no such inode.."));
                byte[] fileHandle = NFSFileHandle.builder().inodeId(inode.getNodeId()).fsid(dirFileHandle.getFsid()).build().toHexArray();

                NfsFileHandle3 nfsFileHandle3 = NfsFileHandle3.builder().handleOfLength(fileHandle.length).fileHandle(fileHandle).build();
                PostOpFileHandle3 obj = PostOpFileHandle3.builder().handleFollows(1).nfsFileHandle(nfsFileHandle3).build();
                FAttr3 fAttr3 = inode.toFAttr3();
                fAttr3.setFsidMajor(0);
                fAttr3.setFsidMinor((int)dirFileHandle.getFsid());

                // PostOpAttr objAttr = PostOpAttr.builder().attributesFollow(1).attributes(fAttr3).build();
                // WccAttr wccAttr = WccAttr.builder().size(fAttr3.getSize()).ctimeSeconds(fAttr3.getCtimeSeconds()).ctimeNSeconds(fAttr3.getCtimeNseconds())
                //         .mtimeSeconds(fAttr3.getMtimeSeconds()).mtimeNSeconds(fAttr3.getMtimeNseconds()).build();
                // PreOpAttr preOpAttr = PreOpAttr.builder().attributesFollow(1).attributes(wccAttr).build();
                //         PostOpAttr postOpAttr = PostOpAttr.builder().attributesFollow(fAttr3 != null ? 1 : 0).attributes(fAttr3).build();
                PostOpAttr objAttr = PostOpAttr.builder().attributesFollow(1).attributes(fAttr3).build();
                PostOpAttr postOpAttr = PostOpAttr.builder().attributesFollow(0).build();
                PreOpAttr preOpAttr = PreOpAttr.builder().attributesFollow(0).build();
                WccData wccData = WccData.builder().before(preOpAttr).after(postOpAttr).build();
                MKDIR3resok mkdir3resok = MKDIR3resok.builder().obj(obj).objAttributes(objAttr).dirWcc(wccData).build();
                mkdir3res = MKDIR3res.createOk(mkdir3resok);
            } else {
                PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
                PostOpAttr after = PostOpAttr.builder().attributesFollow(0).build();
                MKDIR3resfail mkdir3resfail = MKDIR3resfail.builder()
                        .dirWcc(WccData.builder().before(before).after(after).build())
                        .build();
                mkdir3res = MKDIR3res.createFail(NfsStat3.NFS3ERR_IO, mkdir3resfail);
            }
        } catch (Exception e) {
            // TODO: handle exception
            log.error("operate db fail..");
                PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
                PostOpAttr after = PostOpAttr.builder().attributesFollow(0).build();
                MKDIR3resfail mkdir3resfail = MKDIR3resfail.builder()
                        .dirWcc(WccData.builder().before(before).after(after).build())
                        .build();
                mkdir3res = MKDIR3res.createFail(NfsStat3.NFS3ERR_IO, mkdir3resfail);
        }

        int rpcNfsLength = mkdir3res.getSerializedSize();

        ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
        rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);
        mkdir3res.serialize(rpcNfsBuffer);

        // Record marking
        int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);

        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcHeaderLength + rpcNfsLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
        fullResponseBuffer.putInt(recordMarkValue);
        fullResponseBuffer.put(rpcHeaderBuffer.array());
        fullResponseBuffer.put(rpcNfsBuffer.array());

        return fullResponseBuffer.array();
    }

    private byte[] createNfsReadDirPlusReply(int xid, Buffer request, int startOffset) throws IOException {
        // Parse directory file handle from request
        int dirFhandleLength = request.getInt(startOffset);
        log.info("Directory handle length: {}", Optional.of(dirFhandleLength));
        byte[] dirFhandle = request.slice(startOffset + 4, startOffset + 4 + dirFhandleLength).getBytes();

        // Parse cookie from request (we'll use this to determine the page)
        int cookieOffset = startOffset + 4 + dirFhandleLength;
        log.info("Reading cookie at offset: {}, dirFhandleLength: {}", Optional.of(cookieOffset), Optional.of(dirFhandleLength));

        long cookie = request.getLong(cookieOffset);
        log.info("Received READDIRPLUS request with cookie: {}", Optional.of(cookie));

        int cookieVeriferOffset = cookieOffset + 8;
        int dircount = request.getInt(cookieVeriferOffset + 8);
        int maxcount = request.getInt(cookieVeriferOffset + 12);
        log.info("READDIRPLUS request parameters - dircount: {} bytes, maxcount: {} bytes", Optional.of(dircount), Optional.of(maxcount));

        // Create reply
        int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
        ByteBuffer rpcHeaderBuffer = RpcUtil.createAcceptedSuccessReplyHeaderBuffer(xid);

        // Define directory entries (simulating a large directory)
        NFSFileHandle dirNfsFileHandle = NFSFileHandle.fromHexArray(dirFhandle);
        Optional<Inode> dirInodeOptional = MyRocksDB.getINodeMetaData(
                MetaKeyUtils.getTargetVnodeId(BUCK_NAME),
                BUCK_NAME,
                dirNfsFileHandle.getInodeId());

        List<Entryplus3> entries = new ArrayList<>();
        READDIRPLUS3res readdirplus3res = null; 

        String dir = "";
        FAttr3 dirFAttr3 = rootDirAttributes;
        if (dirInodeOptional.isPresent()) {
            Inode dirInode = dirInodeOptional.get();
            dirFAttr3 = dirInode.toFAttr3();
            dir = dirInode.getObjName();
        }

        if (dirNfsFileHandle.getInodeId() == 1 ) {
            dir = "/";
        }
        if (dir.length() > 0) {
            log.info("READDIRPLUS target directory: {}, Cookie: {}", Optional.of(dir), cookie);
            List<LatestIndexMetadata> latestIndexMetadatas = MyRocksDB.listDirectoryContents(MetaKeyUtils.getTargetVnodeId(BUCK_NAME), BUCK_NAME, dir);
            Iterator<LatestIndexMetadata> iterator = latestIndexMetadatas.iterator();
            long startCookie = cookie;
            while (iterator.hasNext()) {
                LatestIndexMetadata latestIndexMetadata = iterator.next();
                Optional<Inode> childInodeOptional = MyRocksDB.getINodeMetaData(
                        MetaKeyUtils.getTargetVnodeId(BUCK_NAME),
                        BUCK_NAME,
                        latestIndexMetadata.getInode());

                if (childInodeOptional.isPresent()) {
                    Inode childInode = childInodeOptional.get();
                    FAttr3 nameAttr = childInode.toFAttr3();
                    nameAttr.setFsidMajor(0);
                    nameAttr.setFsidMinor((int)dirNfsFileHandle.getFsid());
                    NFSFileHandle childNfsFileHandle = NFSFileHandle.builder().inodeId(childInode.getNodeId()).fsid(dirNfsFileHandle.getFsid()).build();
                    byte[] childFileHandle = childNfsFileHandle.toHexArray();
                    String entryName =  Paths.get(childInode.getObjName()).getFileName().toString();
                    int entryNameLength = entryName.length();
                    byte[] nameBytes = entryName.getBytes(StandardCharsets.UTF_8);
                    long nextCookie = 0;
                    if (!iterator.hasNext()) {
                        // 注意：
                        // cookie 不返回这个会导致无限循环
                        nextCookie = 0x7fffffffffffffffL;
                    } else {
                        nextCookie = childInode.getNodeId();
                    }
                    Entryplus3 entryplus3 = Entryplus3.builder()
                        .fileid(childInode.getNodeId())
                        .fileNameLength(entryNameLength)
                        .fileName(nameBytes)
                        .cookie(nextCookie)
                        .nameAttrPresent(nameAttr != null ? 1 : 0)
                        .nameAttr(nameAttr)
                        .nameHandlePresent(1)
                        .nameHandleLength(childFileHandle.length)
                        .nameHandle(childFileHandle)
                        .nextEntryPresent(!iterator.hasNext() ? 0 : 1)
                        .build();
                    entries.add(entryplus3);
                }
            }

            if (entries.size() > 0) {
                PostOpAttr dirAttributes = PostOpAttr.builder().attributesFollow(1).attributes(dirFAttr3).build();
                READDIRPLUS3resok readdirplus3resok = READDIRPLUS3resok.builder()
                        .dirAttributes(dirAttributes)
                        .cookieverf(0L)
                        .entriesPresentFlag(1)
                        .entries(entries)
                        .eof(1)
                        .build();
                readdirplus3res = READDIRPLUS3res.createOk(readdirplus3resok);
            } else {
                PostOpAttr dirAttributes = PostOpAttr.builder().attributesFollow(1).attributes(dirFAttr3).build();
                READDIRPLUS3resfail readdirplus3resfail = READDIRPLUS3resfail.builder().dirAttributes(dirAttributes).build();
                readdirplus3res = READDIRPLUS3res.createFail(NfsStat3.NFS3ERR_NOENT, readdirplus3resfail);
            }
        } else {
            PostOpAttr dirAttributes = PostOpAttr.builder().attributesFollow(1).attributes(dirFAttr3).build();
            READDIRPLUS3resfail readdirplus3resfail = READDIRPLUS3resfail.builder().dirAttributes(dirAttributes).build();
            readdirplus3res = READDIRPLUS3res.createFail(NfsStat3.NFS3ERR_SERVERFAULT, readdirplus3resfail);
        }

        int rpcNfsLength = readdirplus3res.getSerializedSize();

        ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
        rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

        readdirplus3res.serialize(rpcNfsBuffer);

        // Record marking
        int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);

        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcHeaderLength + rpcNfsLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
        fullResponseBuffer.putInt(recordMarkValue);
        fullResponseBuffer.put(rpcHeaderBuffer.array());
        fullResponseBuffer.put(rpcNfsBuffer.array());

        return fullResponseBuffer.array();
    }

    private byte[] createNfsFSStatReply(int xid, Buffer request, int startOffset) {
        // Create reply
        final int rpcMessageBodyLength = 24;
        ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
        rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

        // Standard RPC reply header
        rpcBodyBuffer.putInt(xid);
        rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
        rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
        rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
        rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
        rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

        // NFS FSSTAT reply
        // Structure:
        // status (4 bytes)
        // post_op_attr present flag (4 bytes)
        // tbytes (8 bytes)
        // fbytes (8 bytes)
        // abytes (8 bytes)
        // tfiles (8 bytes)
        // ffiles (8 bytes)
        // afiles (8 bytes)
        // invarsec (4 bytes)
        int rpcNfsLength = 4 + // status
                4 + // post_op_attr present flag
                8 + // tbytes
                8 + // fbytes
                8 + // abytes
                8 + // tfiles
                8 + // ffiles
                8 + // afiles
                4;  // invarsec

        ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
        rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

        // Status (NFS_OK = 0)
        rpcNfsBuffer.putInt(0);

        // post_op_attr
        rpcNfsBuffer.putInt(0); // present = false

        // FSSTAT specific fields
        rpcNfsBuffer.putLong(0x0000000000000000L);  // tbytes (total bytes)
        rpcNfsBuffer.putLong(0x0000000000000000L);  // fbytes (free bytes)
        rpcNfsBuffer.putLong(0x0000000000000000L);  // abytes (available bytes)
        rpcNfsBuffer.putLong(0x0000000000000000L);  // tfiles (total files)
        rpcNfsBuffer.putLong(0x0000000000000000L);  // ffiles (free files)
        rpcNfsBuffer.putLong(0x0000000000000000L);  // afiles (available files)
        rpcNfsBuffer.putInt(0);                     // invarsec (invariant seconds)

        // Record marking
        int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
        fullResponseBuffer.putInt(recordMarkValue);
        fullResponseBuffer.put(rpcBodyBuffer.array());
        fullResponseBuffer.put(rpcNfsBuffer.array());

        return fullResponseBuffer.array();
    }

    private byte[] createNfsFSInfoReply(int xid) throws IOException {
        // Standard ONC RPC reply header
        ByteBuffer rpcHeaderBuffer = RpcUtil.createAcceptedSuccessReplyHeaderBuffer(xid);

        // NFS FSINFO reply
        NfsStat3 status = NfsStat3.NFS3_OK;
        FSINFO3resok fsinfo3resok = FSINFO3resok.builder()
                .rtmax(1048576)
                .rtpref(1048576)
                .rtmult(4096)
                .wtmax(1048576)
                .wtpref(1048576)
                .wtmult(512)
                .dtpref(1048576)
                .maxFilesize(0x00000FFFFFFFF000L)
                .seconds(1)
                .nseconds(0)
                .extraField(0x0000001b)
                .build();

        FSINFO3res fsinfo3res = FSINFO3res.createOk(fsinfo3resok);

        int rpcNfsLength = fsinfo3res.getSerializedSize();

        ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
        rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

        fsinfo3res.serialize(rpcNfsBuffer);

        // Record marking
        int recordMarkValue = 0x80000000 | (RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH + rpcNfsLength);

        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH + rpcNfsLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
        fullResponseBuffer.putInt(recordMarkValue);
        fullResponseBuffer.put(rpcHeaderBuffer.array());
        fullResponseBuffer.put(rpcNfsBuffer.array());

        return fullResponseBuffer.array();
    }


    private byte[] createNfsPathConfReply(int xid, Buffer request, int startOffset) throws IOException {
        // Parse file handle from request
        int fhandleLength = request.getInt(startOffset);
        byte[] fhandle = request.slice(startOffset + 4, startOffset + 4 + fhandleLength).getBytes();

        // Create reply
        final int rpcMessageBodyLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
        ByteBuffer rpcBodyBuffer = RpcUtil.createAcceptedSuccessReplyHeaderBuffer(xid);

        // NFS PATHCONF reply
        PostOpAttr objAttributes = PostOpAttr.builder().attributesFollow(0).build();

        PATHCONF3resok pathconf3resok = PATHCONF3resok.builder()
                .objAttributes(objAttributes)
                .linkmax(32000)
                .nameMax(255)
                .noTrunc(1)
                .chownRestricted(0)
                .caseInsensitive(0)
                .casePreserving(1)
                .build();
        PATHCONF3res pathconf3res = PATHCONF3res.createOk(pathconf3resok);

        int rpcNfsLength = pathconf3res.getSerializedSize();
        ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
        rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

        pathconf3res.serialize(rpcNfsBuffer);

        // Record marking
        int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
        fullResponseBuffer.putInt(recordMarkValue);
        fullResponseBuffer.put(rpcBodyBuffer.array());
        fullResponseBuffer.put(rpcNfsBuffer.array());

        return fullResponseBuffer.array();
    }

        private byte[] createNfsRemoveReply(int xid, Buffer request, int startOffset) throws IOException {
        // Parse directory file handle and name from request
        int dirFhandleLength = request.getInt(startOffset);
        byte[] dirFhandle = request.slice(startOffset + 4, startOffset + 4 + dirFhandleLength).getBytes();
        int nameLength = request.getInt(startOffset + 4 + dirFhandleLength);
        String name = request.slice(startOffset + 4 + dirFhandleLength + 4,
                startOffset + 4 + dirFhandleLength + 4 + nameLength).toString("UTF-8");

        // Create reply
        final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
        ByteBuffer rpcHeaderBuffer = RpcUtil.createAcceptedSuccessReplyHeaderBuffer(xid);

        NFSFileHandle dirNfsFileHandle = NFSFileHandle.fromHexArray(dirFhandle);
        Optional<Inode> dirInodeOptional = MyRocksDB.getINodeMetaData(
                MetaKeyUtils.getTargetVnodeId(BUCK_NAME),
                BUCK_NAME,
                dirNfsFileHandle.getInodeId());

        REMOVE3res rmdir3res = null;
        try {
            String dir = "";
            if (dirInodeOptional.isPresent()) {
                Inode parentInode = dirInodeOptional.get();
                dir = parentInode.getObjName();
            }

            if (dirNfsFileHandle.getInodeId() == 1L) {
                dir = "/";
            }

            if (dir.length() > 0) {
                String object = PathUtils.combine(dir, name);
                String targetVnodeId = MetaKeyUtils.getTargetVnodeId(BUCK_NAME);
                Optional<VersionIndexMetadata> vOptional = MyRocksDB.getIndexMetaData(
                        targetVnodeId,
                        BUCK_NAME,
                        object);
                if (vOptional.isPresent()) {
                    long inodeId = vOptional.get().getInode();
                    Inode inode = MyRocksDB.getINodeMetaData(targetVnodeId, BUCK_NAME, inodeId).orElseThrow(() -> new org.rocksdb.RocksDBException("Could not find Inode for " + BUCK_NAME));

                    List<Inode.InodeData> list = inode.getInodeData();

                    for (Inode.InodeData inodeData : list) {
                        if (inodeData.fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                            String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(inode.getBucket(), inodeData.fileName);
                            ChunkFile chunkFile = MyRocksDB.getChunkFileMetaData(chunkKey).orElseThrow(() -> new org.rocksdb.RocksDBException("Could not find chunkFile for " + chunkKey));
                            
                            List<Inode.InodeData> chunkList = chunkFile.getChunkList();
                            for (Inode.InodeData realData : chunkList) {
                                MyRocksDB.deleteFile(realData.fileName);
                            }

                            MyRocksDB.deleteChunkFileMetaData(chunkKey);
                        }
                    }

                    MyRocksDB.deleteAllMetadata(targetVnodeId, BUCK_NAME, object, vOptional.get().getStamp());

                    PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
                    PostOpAttr after = PostOpAttr.builder().attributesFollow(0).build();
                    WccData dirWcc = WccData.builder().before(before).after(after).build();

                    REMOVE3resok rmdir3resok = REMOVE3resok.builder().dirWcc(dirWcc).build();
                    rmdir3res = REMOVE3res.createOk(rmdir3resok);
                } else {
                    PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
                    PostOpAttr after = PostOpAttr.builder().attributesFollow(0).build();
                    WccData dirWcc = WccData.builder().before(before).after(after).build();
                    REMOVE3resfail rmdir3resfail = REMOVE3resfail.builder().dirWcc(dirWcc).build();
                    rmdir3res = REMOVE3res.createFail(NfsStat3.NFS3ERR_NOENT, rmdir3resfail);
                }
            } else {
                PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
                PostOpAttr after = PostOpAttr.builder().attributesFollow(0).build();
                WccData dirWcc = WccData.builder().before(before).after(after).build();
                REMOVE3resfail rmdir3resfail = REMOVE3resfail.builder().dirWcc(dirWcc).build();
                rmdir3res = REMOVE3res.createFail(NfsStat3.NFS3ERR_IO, rmdir3resfail);
            }
        } catch (Exception e) {
            PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
            PostOpAttr after = PostOpAttr.builder().attributesFollow(0).build();
            WccData dirWcc = WccData.builder().before(before).after(after).build();
            REMOVE3resfail rmdir3resfail = REMOVE3resfail.builder().dirWcc(dirWcc).build();
            rmdir3res = REMOVE3res.createFail(NfsStat3.NFS3ERR_IO, rmdir3resfail);
        }

        int rpcNfsLength = rmdir3res.getSerializedSize();
        ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
        rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);
        rmdir3res.serialize(rpcNfsBuffer);

        // Record marking
        int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);

        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcHeaderLength + rpcNfsLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
        fullResponseBuffer.putInt(recordMarkValue);
        fullResponseBuffer.put(rpcHeaderBuffer.array());
        fullResponseBuffer.put(rpcNfsBuffer.array());

        return fullResponseBuffer.array();
    }

    private byte[] createNfsRmdirReply(int xid, Buffer request, int startOffset) throws IOException {
        // Parse directory file handle and name from request
        int dirFhandleLength = request.getInt(startOffset);
        byte[] dirFhandle = request.slice(startOffset + 4, startOffset + 4 + dirFhandleLength).getBytes();
        int nameLength = request.getInt(startOffset + 4 + dirFhandleLength);
        String name = request.slice(startOffset + 4 + dirFhandleLength + 4,
                startOffset + 4 + dirFhandleLength + 4 + nameLength).toString("UTF-8");

        // Create reply
        final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
        ByteBuffer rpcHeaderBuffer = RpcUtil.createAcceptedSuccessReplyHeaderBuffer(xid);

        NFSFileHandle dirNfsFileHandle = NFSFileHandle.fromHexArray(dirFhandle);
        Optional<Inode> dirInodeOptional = MyRocksDB.getINodeMetaData(
                MetaKeyUtils.getTargetVnodeId(BUCK_NAME),
                BUCK_NAME,
                dirNfsFileHandle.getInodeId());

        REMOVE3res rmdir3res = null;
        try {
            String dir = "";
            if (dirInodeOptional.isPresent()) {
                Inode parentInode = dirInodeOptional.get();
                dir = parentInode.getObjName();
            }

            if (dirNfsFileHandle.getInodeId() == 1L) {
                dir = "/";
            }

            if (dir.length() > 0) {
                String object = PathUtils.combine(dir, name);
                String targetVnodeId = MetaKeyUtils.getTargetVnodeId(BUCK_NAME);
                Optional<VersionIndexMetadata> vOptional = MyRocksDB.getIndexMetaData(
                        targetVnodeId,
                        BUCK_NAME,
                        object);
                if (vOptional.isPresent()) {
                    MyRocksDB.deleteAllMetadata(targetVnodeId, BUCK_NAME, object, vOptional.get().getStamp());

                    PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
                    PostOpAttr after = PostOpAttr.builder().attributesFollow(0).build();
                    WccData dirWcc = WccData.builder().before(before).after(after).build();

                    REMOVE3resok rmdir3resok = REMOVE3resok.builder().dirWcc(dirWcc).build();
                    rmdir3res = REMOVE3res.createOk(rmdir3resok);
                } else {
                    PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
                    PostOpAttr after = PostOpAttr.builder().attributesFollow(0).build();
                    WccData dirWcc = WccData.builder().before(before).after(after).build();
                    REMOVE3resfail rmdir3resfail = REMOVE3resfail.builder().dirWcc(dirWcc).build();
                    rmdir3res = REMOVE3res.createFail(NfsStat3.NFS3ERR_NOENT, rmdir3resfail);
                }
            } else {
                PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
                PostOpAttr after = PostOpAttr.builder().attributesFollow(0).build();
                WccData dirWcc = WccData.builder().before(before).after(after).build();
                REMOVE3resfail rmdir3resfail = REMOVE3resfail.builder().dirWcc(dirWcc).build();
                rmdir3res = REMOVE3res.createFail(NfsStat3.NFS3ERR_IO, rmdir3resfail);
            }
        } catch (Exception e) {
            PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
            PostOpAttr after = PostOpAttr.builder().attributesFollow(0).build();
            WccData dirWcc = WccData.builder().before(before).after(after).build();
            REMOVE3resfail rmdir3resfail = REMOVE3resfail.builder().dirWcc(dirWcc).build();
            rmdir3res = REMOVE3res.createFail(NfsStat3.NFS3ERR_IO, rmdir3resfail);
        }

        int rpcNfsLength = rmdir3res.getSerializedSize();
        ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
        rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);
        rmdir3res.serialize(rpcNfsBuffer);

        // Record marking
        int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);

        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcHeaderLength + rpcNfsLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
        fullResponseBuffer.putInt(recordMarkValue);
        fullResponseBuffer.put(rpcHeaderBuffer.array());
        fullResponseBuffer.put(rpcNfsBuffer.array());

        return fullResponseBuffer.array();
    }

    private byte[] createNfsCommitReply(int xid, Buffer request, int startOffset) throws IOException, URISyntaxException {
        // Create reply
        final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
        ByteBuffer rpcHeaderBuffer = RpcUtil.createAcceptedSuccessReplyHeaderBuffer(xid);

        int fileHandleLength = request.getInt(startOffset);
        byte[] fileHandle = request.slice(startOffset + 4, startOffset + 4 + fileHandleLength).getBytes();

        NFSFileHandle nfsFileHandle = NFSFileHandle.fromHexArray(fileHandle);
        Optional<Inode> inodeOptional = MyRocksDB.getINodeMetaData(MetaKeyUtils.getTargetVnodeId(BUCK_NAME), BUCK_NAME, nfsFileHandle.getInodeId());

        COMMIT3res commit3res = null;

        if (inodeOptional.isPresent()) {
            Inode inode = inodeOptional.get();

            FAttr3 attritbutes = inode.toFAttr3();

            PreOpAttr befor = PreOpAttr.builder().attributesFollow(0).build();
            PostOpAttr after = PostOpAttr.builder().attributesFollow(attritbutes != null ? 1 : 0).attributes(attritbutes).build();

            WccData fileWcc = WccData.builder().before(befor).after(after).build();
            COMMIT3resok commit3resok = COMMIT3resok.builder().fileWcc(fileWcc).verifier(0L).build();

            commit3res = COMMIT3res.createSuccess(commit3resok);
        }

        int rpcNfsLength = commit3res.getSerializedSize();
        ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
        rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);
        commit3res.serialize(rpcNfsBuffer);

        // Record marking
        int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);

        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcHeaderLength + rpcNfsLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
        fullResponseBuffer.putInt(recordMarkValue);
        fullResponseBuffer.put(rpcHeaderBuffer.array());
        fullResponseBuffer.put(rpcNfsBuffer.array());

        return fullResponseBuffer.array();
    }


    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();

        System.out.println("Deploying TcpServerVerticle...");
        vertx.deployVerticle(new Nfsv3Server());
    }

}
