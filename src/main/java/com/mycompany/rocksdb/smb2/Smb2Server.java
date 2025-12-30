package com.mycompany.rocksdb.smb2;

import io.reactivex.Flowable;
import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.disposables.Disposable;
import io.vertx.core.Future;
import io.vertx.core.net.NetServerOptions;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.net.NetServer;
import io.vertx.reactivex.core.net.NetSocket;
import io.vertx.reactivex.core.buffer.Buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mycompany.rocksdb.smb2.POJO.Smb2Header;
import com.mycompany.rocksdb.POJO.Inode;
import com.mycompany.rocksdb.nfs4.NfsConstants;
import com.mycompany.rocksdb.smb2.Smb2ConnectionState.Smb2FileHandle;
import com.mycompany.rocksdb.smb2.Smb2Constants;
import com.mycompany.rocksdb.smb2.Smb2RequestContext;
import com.mycompany.rocksdb.smb2.Smb2OperationHandler;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Smb2Server extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(Smb2Server.class);
    private static String HOST = "0.0.0.0";
    private static final int PORT = 445; // Standard SMB2 port
    public static final Inode ROOT_INODE = Inode.builder().build();

    private final CompositeDisposable disposables = new CompositeDisposable();
    private final Map<Smb2Command, Smb2OperationHandler> handlerMap = new ConcurrentHashMap<>();
    public static final Map<Long, String> openHandles = new ConcurrentHashMap<>();


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
                        error -> log.error("SMB2 Server connection error", error)
                );

        disposables.add(serverSubscription);

        server.rxListen(PORT)
                .subscribe(
                        s -> {
                            log.info("SMB2 Server started on host " + HOST + " and port " + s.actualPort());
                            initHandlers(); // Initialize handlers after server setup
                            startPromise.complete();
                        },
                        error -> {
                            log.error("Failed to start SMB2 server", error);
                            startPromise.fail(error);
                        }
                );
    }

    private static final String SHARE_ROOT_PATH = "/tmp/smb2_share";

    private void handleNewConnection(NetSocket socket) {
        log.info("SMB2 Client connected: " + socket.remoteAddress());

        final Smb2ConnectionState connectionState = new Smb2ConnectionState();
        connectionState.putFileHandle(ROOT_INODE.getInodeId(), Paths.get(SHARE_ROOT_PATH, ROOT_INODE.getObjName()).toString());

        Flowable<Buffer> commandStream = socket.toFlowable()
                .compose(new Smb2CommandFramer());

        Disposable socketSubscription = commandStream.subscribe(
                // onNext: Received a complete SMB2 message
                smb2Message -> handleSmb2Command(smb2Message, socket, connectionState),
                // onError: Connection error
                error -> {
                    log.error("Error on SMB2 connection [" + socket.remoteAddress() + "]: ", error);
                    socket.close();
                },
                // onComplete: Client disconnected
                () -> log.info("SMB2 Client disconnected: " + socket.remoteAddress())
        );

        disposables.add(socketSubscription);
    }

    private void handleSmb2Command(Buffer smb2Message, NetSocket socket, Smb2ConnectionState state) {
        log.info("Received SMB2 message ({} bytes).", smb2Message.length());

        try {
            Smb2RequestContext requestContext = new Smb2RequestContext(socket, state, smb2Message);
            
            // 用于存放所有子请求生成的响应片段
            List<Buffer> responseFragments = new ArrayList<>();
            int currentOffset = 0; // 这里的 currentOffset 是指解析请求包的游标
            long reqNextCommand;

            do {
                // 1. 解码当前的 SMB2 Header (注意：解码位置要跟随偏移量)
                Smb2Header header = Smb2Header.decode(smb2Message, currentOffset);
                reqNextCommand = header.getNextCommand(); // 获取请求中的链偏移量
                
                log.info("Processing SMB2 Command: 0x{}, MessageId: {}, Chain Offset: {}", 
                        Integer.toHexString(header.getCommand()), header.getMessageId(), reqNextCommand);

                // 更新上下文（如果你的 requestContext 是共用的，注意清理旧数据）
                requestContext.setMessageId(header.getMessageId());
                requestContext.setCommandCode(header.getCommand());

                // 2. 查找并执行 Handler
                Smb2Command command = Smb2Command.fromValue(header.getCommand());
                Smb2OperationHandler handler = handlerMap.get(command);
                
                Buffer singleResponse;
                if (handler != null) {
                    // handler 应该返回包含 Header 和 Body 的完整 SMB2 报文 (但不带 NetBIOS 头)
                    singleResponse = handler.handle(requestContext, header, currentOffset);
                } else {
                    singleResponse = Smb2Utils.buildErrorResponse(header, Smb2Constants.STATUS_NOT_SUPPORTED);
                }

                if (singleResponse != null) {
                    // 3. 处理复合响应的链指向和对齐
                    if (reqNextCommand != 0) {
                        // 如果请求包后面还有命令，说明这不是最后一个响应
                        // 响应包必须 8 字节对齐
                        int originalSize = singleResponse.length();
                        int paddedSize = (originalSize + 7) & ~7; // 计算 8 字节对齐后的长度
                        
                        // 重要：修改该响应片段 Header 中的 NextCommand (偏移量 20 处)
                        // 这样客户端才知道下一个响应从哪里开始
                        singleResponse.setIntLE(20, paddedSize); 

                        // 如果对齐需要填充字节 (Padding)
                        if (paddedSize > originalSize) {
                            singleResponse.appendBytes((new byte[paddedSize - originalSize]));
                        }
                    } else {
                        // 如果是最后一个请求，对应的响应 NextCommand 必须为 0
                        singleResponse.setIntLE(20, 0);
                    }
                    
                    responseFragments.add(singleResponse);
                }

                // 4. 移动解析游标到下一个请求
                if (reqNextCommand != 0) {
                    currentOffset += reqNextCommand;
                }

            } while (reqNextCommand != 0);

            // --- 5. 循环结束后，统一封装并发送 ---
            if (!responseFragments.isEmpty()) {
                Buffer finalPayload = Buffer.buffer();
                for (Buffer frag : responseFragments) {
                    finalPayload.appendBuffer(frag);
                }

                // 添加 NetBIOS 4 字节头部 (Type 0x00 + 3字节长度)
                Buffer netbiosPacket = Buffer.buffer();
                netbiosPacket.appendByte((byte) 0x00);
                netbiosPacket.appendMedium(finalPayload.length());

                socket.write(netbiosPacket.appendBuffer(finalPayload));
            }

        } catch (IllegalArgumentException e) {
            log.error("Error decoding SMB2 message: {}", e.getMessage());
            // TODO: Send an SMB2 error response for bad message
            socket.write(buildErrorResponse(0, 0, 0, Smb2Constants.STATUS_INVALID_PARAMETER)); // Cannot get message ID if header is bad
        } catch (Exception e) {
            log.error("Error handling SMB2 command: ", e);
            // TODO: Send a generic SMB2 error response
            socket.write(buildErrorResponse(0, 0, 0, Smb2Constants.STATUS_INTERNAL_ERROR)); // Assuming an internal error status
        }
    }

    private void initHandlers() {
        handlerMap.put(Smb2Command.SMB2_NEGOTIATE, new Smb2NegotiateHandler());
        handlerMap.put(Smb2Command.SMB2_SESSION_SETUP, new Smb2SessionSetupHandler());
        handlerMap.put(Smb2Command.SMB2_TREE_CONNECT, new Smb2TreeConnectHandler());
        handlerMap.put(Smb2Command.SMB2_CREATE, new Smb2CreateHandler());
        handlerMap.put(Smb2Command.SMB2_IOCTL, new Smb2IoctlHandler());
        handlerMap.put(Smb2Command.SMB2_GET_INFO, new Smb2GetInfoHandler());
        handlerMap.put(Smb2Command.SMB2_QUERY_DIRECTORY, new Smb2QueryDirectoryHandler());
        handlerMap.put(Smb2Command.SMB2_CLOSE, new Smb2CloseHandler());
        handlerMap.put(Smb2Command.SMB2_READ, new Smb2ReadHandler());
        handlerMap.put(Smb2Command.SMB2_ECHO, new Smb2EchoHandler());
        handlerMap.put(Smb2Command.SMB2_FLUSH, new Smb2FlushHandler());

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

    private Buffer buildErrorResponse(long messageId, long treeId, long sessionId, long statusCode) {
        Smb2Header errorHeader = new Smb2Header();
        errorHeader.setFlags(Smb2Constants.SMB2_FLAG_RESPONSE);
        errorHeader.setMessageId(messageId);
        errorHeader.setTreeId(treeId);
        errorHeader.setSessionId(sessionId);
        errorHeader.setStatus(statusCode);
        errorHeader.setCommand((short) 0xFFFF); // Indicate error response, no specific command
        errorHeader.setCreditRequestResponse((short) 1); // 分配 1 个信用

        Buffer headerBuffer = errorHeader.encode();

        // 2. 构建 SMB2 ERROR Response Body (固定 9 bytes)
        // 结构如下：
        // - StructureSize (2 bytes): 固定为 9 (0x0009)
        // - ErrorContextCount (1 byte): 通常为 0
        // - Reserved (1 byte): 固定 0
        // - ByteCount (4 bytes): 通常为 0
        // - ErrorData (1 byte): 填充位，通常为 0
        ByteBuffer errorBody = ByteBuffer.allocate(9).order(java.nio.ByteOrder.LITTLE_ENDIAN); 
        errorBody.putShort((short) 9);      // StructureSize: 0x0009
        errorBody.put((byte) 0);        // ErrorContextCount
        errorBody.put((byte) 0);        // Reserved
        errorBody.putInt(0);                // ByteCount
        errorBody.put((byte) 0);        // ErrorData / Padding

        // 3. 合并 Header 和 Body 并返回完整的响应

        return headerBuffer.appendBytes(errorBody.array());
    }
}
