package com.mycompany.rocksdb.netserver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mycompany.rocksdb.POJO.ChunkFile;
import com.mycompany.rocksdb.POJO.Inode;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.mycompany.rocksdb.constant.GlobalConstant.BLOCK_SIZE;
import static com.mycompany.rocksdb.utils.MetaKeyUtils.getRequestId;


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


    private static final MyRocksDB myRocksDB = new MyRocksDB();
    private static final String INDEX_LUN = "fs-SP0-8-index";
    private static final String DATA_LUN = "fs-SP0-2";
    private static final Path SAVE_DATA_PATH = Paths.get("/dev/sdg2");
    public static final String BUCK_NAME = "1212";

    //private static final SimpleRSocketClient simpleRSocketClient = new SimpleRSocketClient(CONJUGATE_RSOCKET_PORT, RSOCKET_PORT);

    public static MyRocksDB getMyRocksDB() {
        return myRocksDB;
    }

    private void init() throws DecoderException, URISyntaxException {
        // Current time in seconds and nanoseconds
        long currentTimeMillis = System.currentTimeMillis();
        int seconds = (int) (currentTimeMillis / 1000);
        int nseconds = (int) ((currentTimeMillis % 1000) * 1_000_000);
        FAttr3 objAttributes = FAttr3.builder()
                .type(2)
                .mode(0755)
                .nlink(1)
                .uid(0)
                .gid(0)
                .size(4096)
                .used(4096)
                .rdev(0)
                .fsidMajor(0x08c60040)
                .fsidMinor(0x2b5cd8a8)
                .fileid(33554434)
                .atimeSeconds(seconds)
                .atimeNseconds(nseconds)
                .mtimeSeconds(seconds)
                .mtimeNseconds(nseconds)
                .ctimeSeconds(seconds)
                .ctimeNseconds(nseconds)
                .build();

        String dataLiteral = "0100070002000002000000003e3e7dae34c9471896e6218574c98110";
        byte[] filehandle = NetTool.hexStringToByteArray(dataLiteral);
        ByteArrayKeyWrapper byteArrayKeyWrapper = new ByteArrayKeyWrapper(filehandle);
        fileHandleToFAttr3.put(byteArrayKeyWrapper, objAttributes);

        Path staticRootPath = Paths.get(STATIC_FILES_ROOT);
        if (!Files.exists(staticRootPath)) {
            try {
                Files.createDirectories(staticRootPath);
            } catch (Exception e) {
                return;
            }
        }


        //myRocksDB.init();
    }
    @Override
    public void start(Future<Void> startFuture) throws Exception {
        init();

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
//                case NFSPROC_REMOVE:
//                    xdrReplyBytes = createNfsRemoveReply(xid, buffer, startOffset);
//                    break;
//                case NFSPROC_RMDIR:
//                    xdrReplyBytes = createNfsRmdirReply(xid, buffer, startOffset);
//                    break;
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

    private static int getFileType(String name) {
        int fileType;
        if (name.endsWith("/") || name.equals("docs") || name.equals("src") ||
                name.equals("bin") || name.equals("lib") || name.equals("include") ||
                name.equals("share") || name.equals("etc") || name.equals("var") ||
                name.equals("tmp") || name.equals("usr") || name.equals("home") ||
                name.equals("root") || name.equals("boot") || name.equals("dev") ||
                name.equals("proc") || name.equals("sys") || name.equals("mnt")) {
            fileType = 2;  // NF3DIR = 2, directory
        } else {
            fileType = 1;  // NF3REG = 1, regular file
        }
        return fileType;
    }

    private byte[] createNfsGetAttrReply(int xid, Buffer request, int startOffset) throws IOException {
        // Parse file handle from request
        int fhandleLength = request.getInt(startOffset);
        byte[] fhandle = request.slice(startOffset + 4, startOffset + 4 + fhandleLength).getBytes();

        // Create reply
        final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
        ByteBuffer rpcHeaderBuffer = RpcUtil.createAcceptedSuccessReplyHeaderBuffer(xid);

        long fileId = fileHandleToFileId.getOrDefault(new ByteArrayKeyWrapper(fhandle), Long.valueOf(0x0000000002000002L));
        String filename = fileIdToFileName.getOrDefault(Optional.of(fileId), "/");
        int fileType = getFileType(filename);

        FAttr3 objAttributes = fileHandleToFAttr3.getOrDefault(new ByteArrayKeyWrapper(fhandle), null);

        GETATTR3res getattr3res = null;
        if (objAttributes != null) {

            GETATTR3resok getattr3resok = GETATTR3resok.builder()
                    .objAttributes(objAttributes)
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
        //FAttr3 attributes = fileHandleToFAttr3.get();
        ByteArrayKeyWrapper byteArrayKeyWrapper = new ByteArrayKeyWrapper(setattr3args.getObject().getFileHandle());
        fileHandleToFAttr3.computeIfPresent(byteArrayKeyWrapper, (key, value) -> {
            SetAttr3 newAttributes = setattr3args.getNewAttributes();
            int modeSetIt = newAttributes.getMode().getSetIt();
            if (modeSetIt != 0) {
                int mode = newAttributes.getMode().getMode();
                value.setMode(mode);
            }
            int uidSetIt = newAttributes.getUid().getSetIt();
            if (uidSetIt != 0) {
                int uid = newAttributes.getUid().getUid();
                value.setUid(uid);
            }
            int gidSetIt = newAttributes.getGid().getSetIt();
            if (gidSetIt != 0) {
                int gid = newAttributes.getGid().getGid();
                value.setGid(gid);
            }
            int sizeSetIt = newAttributes.getSize().getSetIt();
            if (sizeSetIt != 0) {
                long size = newAttributes.getSize().getSize();
                value.setSize(size);
            }
            int atimeSetToServerTime = newAttributes.getAtime();
            int mtimeSetToServerTIme = newAttributes.getMtime();
            long currentTimeMillis = System.currentTimeMillis();
            int seconds = (int)(currentTimeMillis / 1000);
            int nseconds = (int)((currentTimeMillis % 1000) * 1_000_000);
            if (atimeSetToServerTime != 0) {
                value.setAtimeSeconds(seconds);
                value.setAtimeNseconds(nseconds);
            }
            if (mtimeSetToServerTIme != 0) {
                value.setMtimeSeconds(seconds);
                value.setMtimeNseconds(nseconds);
            }

            return value;
        });
        FAttr3 attributes = fileHandleToFAttr3.get(byteArrayKeyWrapper);

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

        SETATTR3res setattr3res = SETATTR3res.createSuccess(setattr3resok);

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

    // Helper method to generate a unique file handle based on the file name
    private ByteArrayKeyWrapper generateFileHandle(String name) {
        // Create a 32-byte file handle
        byte[] handle = new byte[32];
        // Fill with zeros initially
        for (int i = 0; i < handle.length; i++) {
            handle[i] = 0;
        }

        // Use the first 8 bytes for a hash of the name
        int hash = name.hashCode();
        handle[0] = (byte)(hash >> 24);
        handle[1] = (byte)(hash >> 16);
        handle[2] = (byte)(hash >> 8);
        handle[3] = (byte)hash;

        // Use the next 4 bytes for the file type (1 for regular file, 2 for directory)
        int fileType = name.endsWith("/") || name.equals("docs") || name.equals("src") ||
                name.equals("bin") || name.equals("lib") || name.equals("include") ||
                name.equals("share") || name.equals("etc") || name.equals("var") ||
                name.equals("tmp") || name.equals("usr") || name.equals("home") ||
                name.equals("root") || name.equals("boot") || name.equals("dev") ||
                name.equals("proc") || name.equals("sys") || name.equals("mnt") ? 2 : 1;
        handle[4] = (byte)fileType;

        // Use the next 4 bytes for a timestamp
        long timestamp = System.currentTimeMillis();
        handle[8] = (byte)(timestamp >> 56);
        handle[9] = (byte)(timestamp >> 48);
        handle[10] = (byte)(timestamp >> 40);
        handle[11] = (byte)(timestamp >> 32);

        return new ByteArrayKeyWrapper(handle);
    }

    private byte[] getFileHandle(String name, boolean createFlag) {
        if(createFlag && !fileNameTofileHandle.containsKey(name)) {
            fileNameTofileHandle.put(name, generateFileHandle(name));
        }

        ByteArrayKeyWrapper fileHandleByteArrayWrapper = fileNameTofileHandle.get(name);
        if (fileHandleByteArrayWrapper == null) {
            return new byte[0];
        }
        return fileHandleByteArrayWrapper.getData();
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
        byte[] fileHandle = getFileHandle(name, false);

        int fileHandleLength = fileHandle.length;

        LOOKUP3res lookup3res = null;
        if (fileHandleLength > 0) {
            FAttr3 fAttr3 = fileHandleToFAttr3.getOrDefault(new ByteArrayKeyWrapper(fileHandle), null);
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
        long fileId = fileHandleToFileId.getOrDefault(new ByteArrayKeyWrapper(fhandle), Long.valueOf(0x0000000002000002L));
        String filename = fileIdToFileName.getOrDefault(Optional.of(fileId), "/");
        // File attributes
        int fileType =  getFileType(filename);
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

        ACCESS3res access3res = ACCESS3res.createOk(access3resok);

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
            cur.offset = calOffset;
            calOffset += cur.size;

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

        ByteBuffer buffer = ByteBuffer.allocate(count);

        // Create reply
        final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
        ByteBuffer rpcBodyBuffer = RpcUtil.createAcceptedSuccessReplyHeaderBuffer(xid);

        long fileId = fileHandleToFileId.getOrDefault(new ByteArrayKeyWrapper(fhandle), Long.valueOf(0x0000000002000002L));
        String name = fileIdToFileName.getOrDefault(Optional.of(fileId), "/");
        int fileType =  getFileType(name);
        ByteArrayKeyWrapper byteArrayKeyWrapper = new ByteArrayKeyWrapper(fhandle);
        String requestId = fileHandleToRequestId.get(byteArrayKeyWrapper);
        //long fileOffset = fileHandleToOffset.get(byteArrayKeyWrapper).get(offset);

        String bucket = BUCK_NAME;

        Optional<READ3res> read3resOptinal = Optional.empty();
        Inode inode = fileHandleToINode.get(byteArrayKeyWrapper);
        if (inode != null) {
            List<Inode.InodeData> list = inode.getInodeData();
            if (!list.isEmpty()) {
                Inode.InodeData last = list.get(list.size() - 1);
                String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(inode.getBucket(), last.fileName);
                ChunkFile chunkFile = myRocksDB.getChunkFileMetaData(chunkKey).orElseThrow(() -> new RuntimeException("chunk file not found..."));

                List<Inode.InodeData> readChunk = partialRead(chunkFile, readOffset, count);

                if (readChunk.size() > 0) {
                    List<byte[]> allData = new ArrayList<>();
                    for (Inode.InodeData inodeData : readChunk) {
                        if (!StringUtils.isBlank(inodeData.getFileName())) {
                            FileMetadata fileMetadata = myRocksDB.getFileMetaData(inodeData.getFileName()).orElseThrow(() -> new RuntimeException("readChunk failed..."));
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

                    FAttr3 fAttr3 = fileHandleToFAttr3.getOrDefault(new ByteArrayKeyWrapper(fhandle), null);
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
                    read3resOptinal = Optional.of(READ3res.createFail(NfsStat3.NFS3ERR_NOENT, read3resfail));
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

    private List<byte[]> readSegment(List<Long> offsets, List<Long> lens, long size) throws IOException {
        if (!SAVE_DATA_PATH.toFile().exists()) {
            log.error("No such path exists...");
            throw new IOException("No such path exists...");
        }

        List<byte[]> result = new ArrayList<>();

        for (int i = 0; i < offsets.size() && size > 0; i++) {
            long offset = offsets.get(i);
            long len = lens.get(i);
            int length = (int)Math.min(len, size);

            try(FileChannel fileChannel = FileChannel.open(SAVE_DATA_PATH, StandardOpenOption.READ)) {
                ByteBuffer buffer = ByteBuffer.allocate(length);
                fileChannel.read(buffer, offset);
                buffer.flip();
                byte[] data = new byte[buffer.remaining()];
                buffer.get(data);

                result.add(data);
            }
            size -= len;
        }

        return result;
    }

    public static void creatHole(List<Inode.InodeData> list, Inode.InodeData holeFile , Inode inode) throws JsonProcessingException {
        String holeFileName = MetaKeyUtils.getHoleFileName(inode.getBucket(), getRequestId());
        Inode.InodeData holeChunk = ChunkFile.newChunk(holeFileName, holeFile, inode);
        ChunkFile holeChunkFile =  ChunkFile.builder().nodeId(inode.getNodeId()).bucket(inode.getBucket()).objName(inode.getObjName())
            .versionId(inode.getVersionId()).versionNum(MetaKeyUtils.getVersionNum()).size(holeFile.size).chunkList(new ArrayList<>()).hasDeleteFiles(new LinkedList<>()).build();

        holeChunkFile.getChunkList().add(holeFile);
        list.add(holeChunk);

        long totalSize = 0;
        for (Inode.InodeData d : inode.getInodeData()) {
            totalSize += d.getSize();
        }
        inode.setSize(totalSize);

        totalSize = 0;
        for (Inode.InodeData d : holeChunkFile.getChunkList()) {
            totalSize += d.getSize();
        }
        holeChunkFile.setSize(totalSize);

        String targetVnodeId = MetaKeyUtils.getTargetVnodeId(inode.getBucket());
        myRocksDB.saveINodeMetaData(targetVnodeId, inode);
        myRocksDB.saveChunkFileMetaData(ChunkFile.getChunkKey(holeFileName), holeChunkFile);
    }

    public static void createData(List<Inode.InodeData> list, Inode.InodeData data, Inode inode, byte[] dataToWrite) throws JsonProcessingException {
        String chunkKey = ChunkFile.getChunkKey(data.fileName);
        Inode.InodeData chunk = ChunkFile.newChunk(data.fileName, data, inode);
        ChunkFile chunkFile = ChunkFile.builder().nodeId(inode.getNodeId()).bucket(inode.getBucket()).objName(inode.getObjName())
                .versionId(inode.getVersionId()).versionNum(MetaKeyUtils.getVersionNum()).size(data.size).chunkList(new ArrayList<>()).hasDeleteFiles(new LinkedList<>()).build();

        String targetVnodeId = MetaKeyUtils.getTargetVnodeId(inode.getBucket());

        list.add(chunk);

        long totalSize = 0;
        for (Inode.InodeData d : chunkFile.getChunkList()) {
            totalSize += d.getSize();
        }
        chunkFile.setSize(totalSize);
        totalSize = 0;
        for (Inode.InodeData d : inode.getInodeData()) {
            totalSize += d.getSize();
        }
        inode.setSize(totalSize);


        myRocksDB.saveINodeMetaData(targetVnodeId, inode);
        myRocksDB.saveChunkFileMetaData(chunkKey, chunkFile);
    }

    public static void appendData(List<Inode.InodeData> list, Inode.InodeData data, Inode inode, byte[] dataToWrite) throws JsonProcessingException {
        if (list.isEmpty()) {
            createData(list, data, inode, dataToWrite);
        }
        else {
            Inode.InodeData last = list.get(list.size() - 1);
            String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(inode.getBucket(), last.fileName);
            ChunkFile chunkFile = myRocksDB.getChunkFileMetaData(chunkKey).orElseThrow(() -> new RuntimeException("chunk file not found..."));
            chunkFile.getChunkList().add(data);
            myRocksDB.saveChunkFileMetaData(chunkKey, chunkFile);

            String vnodeId = data.fetchInodeDataTargetVnodeId();
            List<Long> link = Arrays.asList(((long) Long.parseLong(vnodeId)));
            String s_uuid = "0002";
            myRocksDB.saveRedis(vnodeId, link, s_uuid);

            String targetVnodeId = MetaKeyUtils.getTargetVnodeId(inode.getBucket());
            String verisonKey = MetaKeyUtils.getVersionMetaDataKey(targetVnodeId, inode.getBucket(), inode.getObjName(), null);
            myRocksDB.saveFileMetaData(data.getFileName(), verisonKey, dataToWrite, dataToWrite.length, true);

            long totalSize = 0;
            int chunkNum = 0;
            for (Inode.InodeData d : chunkFile.getChunkList()) {
                totalSize += d.getSize();
                if (!inode.getInodeData().contains(d)) {
                    //Inode.InodeData newchunk = ChunkFile.newChunk(inodeData.fileName, inodeData, inode);
                    //inode.getInodeData().add(newchunk);
                    chunkNum++;
                }
                //chunkNum++;
            }


//                totalSize = 0;
//                for (Inode.InodeData d : inode.getInodeData()) {
//                    totalSize += d.getSize();
//                    //chunkNum++;
//                }
            inode.setSize(totalSize);
            chunkFile.setSize(totalSize);
            Inode.InodeData pivot = inode.getInodeData().get(0);
            pivot.setChunkNum(chunkNum);
            pivot.setSize(totalSize);

            myRocksDB.saveINodeMetaData(targetVnodeId, inode);
            myRocksDB.saveChunkFileMetaData(chunkKey, chunkFile);
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

        //log.info("NFS Write Reply: {}", new String(data, StandardCharsets.UTF_8));

        // Create reply
        final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
        ByteBuffer rpcBodyBuffer = RpcUtil.createAcceptedSuccessReplyHeaderBuffer(xid);

//    Path staticRootPath = Paths.get(STATIC_FILES_ROOT);
//    Files.write(staticRootPath.resolve(Base64.getUrlEncoder().withoutPadding().encodeToString(fhandle)), data);

        ByteArrayKeyWrapper byteArrayKeyWrapper = new ByteArrayKeyWrapper(fhandle);
        WRITE3res write3res = null;
        FAttr3 attributes = fileHandleToFAttr3.getOrDefault(byteArrayKeyWrapper, null);
        String object = fileHandleToFileName.get(byteArrayKeyWrapper);
        Inode inode = fileHandleToINode.get(byteArrayKeyWrapper);

        if (attributes != null && object != null) {

            fileHandleToOffset.compute(byteArrayKeyWrapper, (key, value) -> {
               if (value == null) {
                   value = new ConcurrentSkipListMap<>();
               }
               value.put(Long.valueOf(reqOffset),  Long.valueOf(dataOfLength));
               return value;
            });

            String bucket = BUCK_NAME;
            //String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);
            String filename = MetaKeyUtils.getObjFileName(bucket, object, getRequestId());
            //String versionId = "null";
            //String versionKey = MetaKeyUtils.getVersionMetaDataKey(targetVnodeId, bucket, object, versionId);

            Inode.InodeData inodeData = new Inode.InodeData();
            inodeData.offset = reqOffset;
            inodeData.size = dataOfLength;
            inodeData.fileName = filename;

            List<Inode.InodeData> list = inode.getInodeData();

            if (list.isEmpty()) {
                createData(list, inodeData, inode, dataToWrite);
            }
            Inode.InodeData last = list.get(list.size()-1);
            String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(inode.getBucket(), last.fileName);
            ChunkFile chunkFile = myRocksDB.getChunkFileMetaData(chunkKey).orElseThrow(() -> new RuntimeException("chunk file not found..."));
            Inode.partialOverwrite3(chunkFile, reqOffset, inodeData);

            String vnodeId = inodeData.fetchInodeDataTargetVnodeId();
            List<Long> link = Arrays.asList(((long) Long.parseLong(vnodeId)));
            String s_uuid = "0002";
            myRocksDB.saveRedis(vnodeId, link, s_uuid);

            String targetVnodeId = MetaKeyUtils.getTargetVnodeId(inode.getBucket());
            String verisonKey = MetaKeyUtils.getVersionMetaDataKey(targetVnodeId, inode.getBucket(), inode.getObjName(), inode.getVersionId());
            myRocksDB.saveFileMetaData(inodeData.getFileName(), verisonKey, dataToWrite, dataToWrite.length,true);

            long totalSize = 0;
            int chunkNum = 0;
            for (Inode.InodeData d : chunkFile.getChunkList()) {
                totalSize += d.getSize();
                if (!inode.getInodeData().contains(d)) {
                    chunkNum++;
                }

            }
            inode.setSize(totalSize);
            chunkFile.setSize(totalSize);
            Inode.InodeData pivot = inode.getInodeData().get(0);
            pivot.setChunkNum(chunkNum);
            pivot.setSize(totalSize);

            myRocksDB.saveINodeMetaData(targetVnodeId, inode);
            myRocksDB.saveChunkFileMetaData(chunkKey, chunkFile);

            FAttr3 attritbutes = fileHandleToFAttr3.computeIfPresent(byteArrayKeyWrapper, (key, value) -> {
                synchronized (value) {
                    long used = inode.getSize() / BLOCK_SIZE * BLOCK_SIZE + BLOCK_SIZE;
                    value.setSize(inode.getSize());
                    value.setUsed(used);
                }
                return value;
            });


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

    // Helper method to generate a unique file ID based on the file name
    private long generateFileId(String name) {
        // Use a combination of name hash and timestamp to create a unique ID
        int hash = name.hashCode();
        long timestamp = System.currentTimeMillis();
        return ((long)hash << 32) | (timestamp & 0xFFFFFFFFL);
    }

    private long getFileId(String name) {
        if(!fileNameTofileId.containsKey(name)) {
            long fileId = generateFileId(name);
            fileNameTofileId.put(name, Long.valueOf(fileId));
        }

        long fileId = fileNameTofileId.get(name);
        return fileId;
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

        byte[] fileHandle = getFileHandle(name, true);

        int fileHandleLength = fileHandle.length;

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
        int fileType;
        fileType = getFileType(name);
        long currentTimeMillis = System.currentTimeMillis();
        int seconds = (int) (currentTimeMillis / 1000);
        int nseconds = (int) ((currentTimeMillis % 1000) * 1_000_000);
        long fileId = getFileId(name);

        NfsFileHandle3 nfsFileHandle = NfsFileHandle3.builder()
                .handleOfLength(fileHandleLength)
                .fileHandle(fileHandle)
                .build();

        PostOpFileHandle3 obj = PostOpFileHandle3.builder()
                .handleFollows(1)
                .nfsFileHandle(nfsFileHandle)
                .build();

        FAttr3 attributes = FAttr3.builder()
                .type(fileType)
                .mode(0)
                .nlink(1)
                .uid(0)
                .gid(0)
                .size(0)
                .used(0)
                .rdev(0)
                .fsidMajor(0x08c60040)
                .fsidMinor(0x2b5cd8a8)
                .fileid(fileId)
                .ctimeSeconds(seconds)
                .ctimeNseconds(nseconds)
                .build();
        PostOpAttr ojbAttributes = PostOpAttr.builder().attributesFollow(1).attributes(attributes).build();

        PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
        PostOpAttr after = PostOpAttr.builder().attributesFollow(0).build();
        WccData dirWcc = WccData.builder().before(before).after(after).build();

        CREATE3resok create3resok = CREATE3resok.builder()
                .obj(obj)
                .ojbAttributes(ojbAttributes)
                .dirWcc(dirWcc)
                .build();

        CREATE3res create3res = CREATE3res.createSuccess(create3resok);

        int rpcNfsLength = create3res.getSerializedSize();
        ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
        rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

        create3res.serialize(rpcNfsBuffer);

        // 哈希表暂时保存数据
        fileHandleToFileId.put(new ByteArrayKeyWrapper(fileHandle), Long.valueOf(fileId));
        fileHandleToFileName.put(new ByteArrayKeyWrapper(fileHandle), name);
        fileIdToFileName.put(Long.valueOf(fileId), name);
        fileIdToFAttr3.put(Long.valueOf(fileId), attributes);
        fileHandleToFAttr3.put(new ByteArrayKeyWrapper(fileHandle), attributes);
        fileHandleToParentFileHandle.put(new ByteArrayKeyWrapper(fileHandle), new ByteArrayKeyWrapper(dirFhandle));
        fileHandleToChildrenFileHandle.compute(new ByteArrayKeyWrapper(dirFhandle), (key, value) -> {
            if (value == null) {
                value = new ArrayList<>();
            }
            boolean add = value.add(new ByteArrayKeyWrapper(fileHandle));
            return value;
        });
        String requestId = getRequestId();
        //fileHandleToRequestId.put(new ByteArrayKeyWrapper(fileHandle), requestId);
//        Inode inode = Inode.builder().linkN(1).atime(0).createTime(seconds).ctime(seconds).mode(0)
//                        .uid(0).gid(0).size(0).objName(name).nodeId(fileId).cookie(fileId+1L).bucket(BUCK_NAME)
//                        .storage("dataa").majorDev(0x08c60040).minorDev(0x2b5cd8a8).versionId("null").reference(name).versionNum(MetaKeyUtils.getVersionNum()).build();



        // 将元数据持久化存储到RocksDB中
        try {
            String bucket = BUCK_NAME;
            String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);
            String object = name;
            String filename = MetaKeyUtils.getObjFileName(bucket, object, requestId);
            Inode inode = myRocksDB.saveIndexMetaAndInodeData(targetVnodeId, bucket, object, filename, 0, "text/plain", true).orElseThrow(() -> new RuntimeException("no such inode.."));

            fileHandleToINode.put(new ByteArrayKeyWrapper(fileHandle), inode);
            //myRocksDB.saveINodeMetaData(targetVnodeId, inode);

            String vnodeId = MetaKeyUtils.getObjectVnodeId(bucket, object);
            List<Long> link = Arrays.asList(((long) Long.parseLong(vnodeId)));
            String s_uuid = "0002";
            myRocksDB.saveRedis(vnodeId, link, s_uuid);

        }catch (Exception e) {
            log.error("operate db fail..");
        }

        // Record marking
        int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);

        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcHeaderLength + rpcNfsLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
        fullResponseBuffer.putInt(recordMarkValue);
        fullResponseBuffer.put(rpcHeaderBuffer.array());
        fullResponseBuffer.put(rpcNfsBuffer.array());

        return fullResponseBuffer.array();
    }

    private byte[] createNfsMkdirReply(int xid, Buffer request, int startOffset) {
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
        int rpcNfsLength = 4 + // status
                32 + // file handle
                4 + // post_op_attr present flag
                4 + // pre_op_attr present flag
                4;  // post_op_attr present flag

        ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
        rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

        // Status (NFS_OK = 0)
        rpcNfsBuffer.putInt(0);

        // file handle (using the same format as MNT reply)
        byte[] fileHandle = "00000000000000010000000000000001000000010000000100000004".getBytes();
        rpcNfsBuffer.put(fileHandle);

        // post_op_attr
        rpcNfsBuffer.putInt(0); // present = false

        // wcc_data
        rpcNfsBuffer.putInt(0); // pre_op_attr present = false
        rpcNfsBuffer.putInt(0); // post_op_attr present = false

        // Record marking
        int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);

        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcHeaderLength + rpcNfsLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
        fullResponseBuffer.putInt(recordMarkValue);
        fullResponseBuffer.put(rpcHeaderBuffer.array());
        fullResponseBuffer.put(rpcNfsBuffer.array());

        return fullResponseBuffer.array();
    }

    private static synchronized long readyToCommitBuffer(ConcurrentSkipListMap<Long, Long> currentData) {
        long expectedOffset = -1, count = 0;
        for (Map.Entry<Long, Long> entry : currentData.entrySet()) {
            if (expectedOffset == -1) {
                expectedOffset = entry.getKey();
            }
            if (expectedOffset != entry.getKey()) {
                log.error("expected offset is not equal to combined offset, expected offset: {}, combined offset: {}", Optional.of(expectedOffset), entry.getKey());
                return -1;
            } else {
                expectedOffset += entry.getValue();
                count += entry.getValue();
            }
        }
        return count;
    }

    private byte[] createNfsReadDirPlusReply(int xid, Buffer request, int startOffset) throws IOException {
        // Parse directory file handle from request
        int dirFhandleLength = request.getInt(startOffset);
        log.info("Directory handle length: {}", Optional.of(dirFhandleLength));
        byte[] dirFhandle = request.slice(startOffset + 4, startOffset + 4 + dirFhandleLength).getBytes();

        // Parse cookie from request (we'll use this to determine the page)
        int cookieOffset = startOffset + 4 + dirFhandleLength;
        log.info("Reading cookie at offset: {}, dirFhandleLength: {}", Optional.of(cookieOffset), Optional.of(dirFhandleLength));

        // Print raw bytes around cookie position for debugging
//        log.info("Raw bytes around cookie position:");
//        for (int i = cookieOffset - 4; i < cookieOffset + 12; i++) {
//            if (i >= 0 && i < request.length()) {
//                log.info("Byte at offset {}: 0x{}", i, String.format("%02X", request.getByte(i)));
//            }
//        }

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
//    List<String> allEntries = fileHandleToChildrenFileHandle.getOrDefault(new ByteArrayKeyWrapper(dirFhandle), new ArrayList<>())
//      .stream()
//      .map(key -> fileHandleToFileName.getOrDefault(key, ""))
//      .filter(s -> !s.isEmpty())
//      .collect(Collectors.toList());

        List<ByteArrayKeyWrapper> allEntries = fileHandleToChildrenFileHandle.getOrDefault(new ByteArrayKeyWrapper(dirFhandle), new ArrayList<>());
        // Calculate size for attributes
        int startIndex = (int) cookie;
        int currentSize = 0;
        int entriesToReturn = 0;
        int nameAttrSize = Nfs3Constant.NAME_ATTR_SIZE;

        long currentTimeMillis = System.currentTimeMillis();
        int seconds = (int)(currentTimeMillis / 1000);
        int nseconds = (int)((currentTimeMillis % 1000) * 1_000_000);

        List<Entryplus3> entries = new ArrayList<>();

        for (int i = 0; i < allEntries.size(); i++) {
            ByteArrayKeyWrapper keyWrapper = allEntries.get(i);
            String entryName = fileHandleToFileName.getOrDefault(keyWrapper, "");
            int entryNameLength = entryName.length();
            long fileId = getFileId(entryName);
            byte[] nameBytes = entryName.getBytes(StandardCharsets.UTF_8);
            long nextCookie = 0;
            if (i == allEntries.size() - 1) {
                // 注意：
                // cookie 不返回这个会导致无限循环
                nextCookie = 0x7fffffffffffffffL;
            } else {
                nextCookie = i + 1;
            }

            log.info("Entry '{}' size: {} bytes, current total: {} bytes (dircount limit: {} bytes)",
                    (Object) entryName, Optional.of(entryNameLength), Optional.of(currentSize), Optional.of(dircount));

            FAttr3 nameAttr = fileHandleToFAttr3.getOrDefault(keyWrapper, null);
            Entryplus3 entryplus3 = Entryplus3.builder()
                    .fileid(fileId)
                    .fileNameLength(entryNameLength)
                    .fileName(nameBytes)
                    .cookie(nextCookie)
                    .nameAttrPresent(nameAttr != null ? 1 : 0)
                    .nameAttr(nameAttr)
                    .nameHandlePresent(1)
                    .nameHandleLength(keyWrapper.getData().length)
                    .nameHandle(keyWrapper.getData())
                    .nextEntryPresent(i == allEntries.size() - 1 ? 0 : 1)
                    .build();

            entries.add(entryplus3);

            currentSize += entryplus3.getSerializedSize();
        }

        log.info("Will return {} entries starting from index {} (total size: {} bytes)",
                Optional.of(entries.size()), Optional.of(0), Optional.of(currentSize));

        FAttr3 dirAttr = fileHandleToFAttr3.getOrDefault(new ByteArrayKeyWrapper(dirFhandle), null);
        int entriesPresentFlag = entries.isEmpty() ? 0 : 1;
        PostOpAttr dirAttributes = PostOpAttr.builder().attributesFollow(1).attributes(dirAttr).build();
        READDIRPLUS3resok readdirplus3resok = READDIRPLUS3resok.builder()
                .dirAttributes(dirAttributes)
                .cookieverf(0L)
                .entriesPresentFlag(entriesPresentFlag)
                .entries(entries)
                .eof(1)
                .build();

        READDIRPLUS3res readdirplus3res = READDIRPLUS3res.createOk(readdirplus3resok);

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

    private byte[] createNfsCommitReply(int xid, Buffer request, int startOffset) throws IOException, URISyntaxException {
        // Create reply
        final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
        ByteBuffer rpcHeaderBuffer = RpcUtil.createAcceptedSuccessReplyHeaderBuffer(xid);

        int fileHandleLength = request.getInt(startOffset);
        byte[] fileHandle = request.slice(startOffset + 4, startOffset + 4 + fileHandleLength).getBytes();

        ByteArrayKeyWrapper keyWrapper = new ByteArrayKeyWrapper(fileHandle);
        AtomicLong nextAppendingPosition = fileHandleNextAppendingPosition.compute(keyWrapper, (k, v) -> {
            if (v == null) return new AtomicLong(0);
            return v;
        });

        String object = fileHandleToFileName.get(keyWrapper);
        //String requestId = fileHandleToRequestId.get(keyWrapper);
        //ConcurrentSkipListMap<Long, Long> currentData = fileHandleToOffset.getOrDefault(keyWrapper, new ConcurrentSkipListMap<Long, Long>());

        COMMIT3res commit3res = null;

        //long count = readyToCommitBuffer(currentData);
        //if (count > 0) {
            String bucket = BUCK_NAME;
            String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);
            //String filename = MetaKeyUtils.getObjFileName(bucket, object, requestId);
            String versionId = "null";
            String versionKey = MetaKeyUtils.getVersionMetaDataKey(targetVnodeId, bucket, object, versionId);
            //Path sourcePath = Paths.get("/mgt/" + filename);
            //long startWriteOffset = currentData.firstKey();
            //long dataOfLength = sourcePath.toFile().length();

//            SocketReqMsg msg = new SocketReqMsg("", 0)
//                    .put("bucket", bucket)
//                    .put("object", object)
//                    .put("filename", filename)
//                    .put("versionKey", versionKey)
//                    .put("targetVnodeId", targetVnodeId)
//                    .put("startWriteOffset", String.valueOf(startWriteOffset))
//                    .put("count", String.valueOf(count))
//                    .put("contentLength", String.valueOf(count))
//                    .put("contentType", "text/plain");
//
//            //myRocksDB.saveIndexMetaData(targetVnodeId, bucket, object, filename, count, "text/plain", false);
//            // 发送元数据
//            simpleRSocketClient.putMetadata(msg).block();
//            // 发送文件数据
//            simpleRSocketClient.uploadLargeFile(sourcePath.toFile().getPath(), msg).block();
//
//            try {
//                Files.deleteIfExists(sourcePath);
//                fileHandleToOffset.remove(keyWrapper);
//            } catch (IOException e) {
//                log.error("Failed to cleanup test files", e);
//            }
            //long fileOffset = myRocksDB.saveFileMetaData(filename, versionKey, sourcePath, startWriteOffset, count, false);
            //sourcePath.toFile().delete();
            //log.info("startWriteOffset: {}, count: {}", startWriteOffset, count);
//            FAttr3 attritbutes = fileHandleToFAttr3.computeIfPresent(keyWrapper, (key, value) -> {
//                synchronized (value) {
//                    long used = count / BLOCK_SIZE * BLOCK_SIZE + BLOCK_SIZE;
//                    value.setSize(value.getSize() + count);
//                    value.setUsed(used);
//                }
//                return value;
//            });

            FAttr3 attritbutes = fileHandleToFAttr3.getOrDefault(keyWrapper, null);

            PreOpAttr befor = PreOpAttr.builder().attributesFollow(0).build();
            PostOpAttr after = PostOpAttr.builder().attributesFollow(attritbutes != null ? 1 : 0).attributes(attritbutes).build();

            WccData fileWcc = WccData.builder().before(befor).after(after).build();
            COMMIT3resok commit3resok = COMMIT3resok.builder().fileWcc(fileWcc).verifier(0L).build();

            commit3res = COMMIT3res.createSuccess(commit3resok);
        //}
//        else if (count == -1) {
//            FAttr3 attritbutes = fileHandleToFAttr3.getOrDefault(keyWrapper, null);
//
//            PreOpAttr befor = PreOpAttr.builder().attributesFollow(0).build();
//            PostOpAttr after = PostOpAttr.builder().attributesFollow(attritbutes != null ? 1 : 0).attributes(attritbutes).build();
//
//            WccData fileWcc = WccData.builder().before(befor).after(after).build();
//            COMMIT3resfail commit3resfail = COMMIT3resfail.builder().fileWcc(fileWcc).build();
//            commit3res = COMMIT3res.createFailure(NfsStat3.NFS3ERR_JUKEBOX, commit3resfail);
//        } else {
//            PreOpAttr befor = PreOpAttr.builder().attributesFollow(0).build();
//            PostOpAttr after = PostOpAttr.builder().attributesFollow(0).build();
//
//            WccData fileWcc = WccData.builder().before(befor).after(after).build();
//            COMMIT3resok commit3resok = COMMIT3resok.builder().fileWcc(fileWcc).verifier(0L).build();
//
//            commit3res = COMMIT3res.createSuccess(commit3resok);
//        }

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
