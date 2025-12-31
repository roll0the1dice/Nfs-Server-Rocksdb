package com.mycompany.rocksdb.netserver;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer; // <-- CHANGED: 使用核心 Buffer
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser; // <-- CHANGED: 使用核心 RecordParser
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public class PortMapperServer extends AbstractVerticle {
    // <-- MODIFIED: Logger 应该关联当前类，而不是 MyRocksDB
    private static final Logger log = LoggerFactory.getLogger(PortMapperServer.class);

    private static final int RPCBIND_PORT = 111;
    private static final int PORTMAPPER_PROGRAM = 100000;
    private static final int PORTMAPPER_VERSION = 2;
    private static final int PMAPPROC_GETPORT = 3;

    private static final int MSG_TYPE_CALL = 0;
    private static final int MSG_TYPE_REPLY = 1;

    private static final int REPLY_STAT_MSG_ACCEPTED = 0;
    private static final int ACCEPT_STAT_SUCCESS = 0;

    private static final int AUTH_NULL_FLAVOR = 0;
    private static final int AUTH_NULL_LENGTH = 0;

    private static final int IPPROTO_TCP = 6;
    private static final int IPPROTO_UDP = 17;

    private static class RpcKey {
        int program;
        int version;
        int protocol;

        RpcKey(int program, int version, int protocol) {
            this.program = program;
            this.version = version;
            this.protocol = protocol;
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RpcKey rpcKey = (RpcKey) o;
            return program == rpcKey.program && version == rpcKey.version && protocol == rpcKey.protocol;
        }
        @Override
        public int hashCode() {
            return Objects.hash(program, version, protocol);
        }
    }

    private final Map<RpcKey, Integer> serviceRegistry = new HashMap<>();
    private RpcParseState currentState = RpcParseState.READING_MARKER;
    private int expectedFragmentLength;
    private boolean isLastFragment;
    private List<Buffer> messageFragments = new ArrayList<>();

    @Override
    public void start(Future<Void> startFuture) {
        // 预注册服务
        registerService(100003, 3, IPPROTO_TCP, 2049);
        registerService(100005, 3, IPPROTO_TCP, 20048);

        // --- TCP Server ---
        NetServerOptions options = new NetServerOptions()
                .setPort(RPCBIND_PORT)
                .setHost("0.0.0.0")
                .setTcpKeepAlive(true);
        NetServer tcpServer = vertx.createNetServer(options);
        tcpServer.connectHandler(this::handleTcpConnection);

        // <-- MODIFIED: 正确处理异步的 listen 操作
        tcpServer.listen(res -> {
            if (res.succeeded()) {
                log.info("TCP Portmapper started successfully on port {}", RPCBIND_PORT);
                startFuture.complete(); // 通知 Vert.x 部署成功
            } else {
                log.error("Failed to start TCP Portmapper", res.cause());
                startFuture.fail(res.cause()); // 通知 Vert.x 部署失败
            }
        });
    }

    public void registerService(int program, int version, int protocol, int port) {
        serviceRegistry.put(new RpcKey(program, version, protocol), port);
        log.info("Registered service: prog=0x{}, vers={}, proto={}, port={}",
                Integer.toHexString(program), version, protocol, port);
    }

    private void handleTcpConnection(NetSocket socket) {
        // 使用核心 RecordParser，创建方式相同
        RecordParser parser = RecordParser.newFixed(4);

        parser.handler(buffer -> {
            if (currentState == RpcParseState.READING_MARKER) {
                int recordMarkerRaw = buffer.getInt(0);
                isLastFragment = (recordMarkerRaw & 0x80000000) != 0;
                expectedFragmentLength = recordMarkerRaw & 0x7FFFFFFF;

                if (expectedFragmentLength == 0) {
                    parser.fixedSizeMode(4);
                    currentState = RpcParseState.READING_MARKER;
                } else {
                    parser.fixedSizeMode(expectedFragmentLength);
                    currentState = RpcParseState.READING_FRAGMENT_DATA;
                }
            } else if (currentState == RpcParseState.READING_FRAGMENT_DATA) {
                messageFragments.add(buffer);

                if (isLastFragment) {
                    processRpcRequest(socket);
                }

                parser.fixedSizeMode(4);
                currentState = RpcParseState.READING_MARKER;
            } else {
                throw new IllegalStateException("Unexpected state: " + currentState);
            }
        });

        // <-- CHANGED: 直接传递 parser，不再需要 .getDelegate()
        socket.handler(parser);
        socket.exceptionHandler(t -> log.error("TCP Socket Exception:", t));

        log.info("SERVER: Handlers set for socket {}", socket.remoteAddress());
    }

    private void processRpcRequest(NetSocket socket) {
        // 使用核心 Buffer，创建方式相同
        Buffer request = Buffer.buffer();
        for (Buffer fragment : messageFragments) {
            request.appendBuffer(fragment);
        }
        messageFragments.clear();

        if (request.length() < 56) {
            log.warn("Request too short: {} bytes", request.length());
            return;
        }

        // 省略日志打印部分...

        try {
            int xid = request.getInt(0);
            int msgType = request.getInt(4);
            int rpcVersion = request.getInt(8);
            int program = request.getInt(12);
            int version = request.getInt(16);
            int procedure = request.getInt(20);

            // --- 动态解析开始 ---
            int currentPos = 24;

            // 1. 跳过 Credentials
            int credFlavor = request.getInt(currentPos);
            int credLength = request.getInt(currentPos + 4);
            // RPC 规定必须 4 字节对齐
            int credPadding = (4 - (credLength % 4)) % 4;
            currentPos += 8 + credLength + credPadding;

            // 2. 跳过 Verifier
            int verfFlavor = request.getInt(currentPos);
            int verfLength = request.getInt(currentPos + 4);
            int verfPadding = (4 - (verfLength % 4)) % 4;
            currentPos += 8 + verfLength + verfPadding;

            // 现在 currentPos 指向了真正的数据参数区 (Procedure Parameters)
            // --- 动态解析结束 ---

            if (msgType == MSG_TYPE_CALL &&
                    program == PORTMAPPER_PROGRAM &&
                    version == PORTMAPPER_VERSION &&
                    procedure == PMAPPROC_GETPORT) {

                // 使用动态计算的偏移量
                int progToLookup = request.getInt(currentPos);
                int versToLookup = request.getInt(currentPos + 4);
                int protToLookup = request.getInt(currentPos + 8);

                log.info("GETPORT request: XID=0x{}, Prog=0x{}, Vers={}, Prot={}",
                        Integer.toHexString(xid), Integer.toHexString(progToLookup), versToLookup, protToLookup);

                int portResult = serviceRegistry.getOrDefault(
                        new RpcKey(progToLookup, versToLookup, protToLookup),
                        0
                );

                log.info("Responding with port: {}", portResult);

                Buffer replyBuffer = createGetPortReply(xid, portResult);

                // 省略日志打印部分...

                // <-- CHANGED: 直接写入 Buffer，不再需要 .getDelegate()
                socket.write(replyBuffer);

            } else {
                log.warn("Ignoring RPC call: XID=0x{}, Prog=0x{}, Vers={}, Proc={}",
                        Integer.toHexString(xid), Integer.toHexString(program), version, procedure);
            }
        } catch (Exception e) {
            log.error("Error processing RPC request", e);
        }
    }

    private Buffer createGetPortReply(int xid, int port) {
        final int rpcMessageBodyLength = 28;
        ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
        rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

        rpcBodyBuffer.putInt(xid);
        rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
        rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
        rpcBodyBuffer.putInt(AUTH_NULL_FLAVOR);
        rpcBodyBuffer.putInt(AUTH_NULL_LENGTH);
        rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);
        rpcBodyBuffer.putInt(port);

        int recordMarkValue = 0x80000000 | rpcMessageBodyLength;

        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
        fullResponseBuffer.putInt(recordMarkValue);
        fullResponseBuffer.put(rpcBodyBuffer.array());

        // 使用核心 Buffer.buffer() 工厂方法
        return Buffer.buffer(fullResponseBuffer.array());
    }

    public static void main(String[] args) {
        io.vertx.core.Vertx.vertx().deployVerticle(new PortMapperServer());
    }
}
