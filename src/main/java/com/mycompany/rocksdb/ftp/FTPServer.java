package com.mycompany.rocksdb.ftp;

import com.mycompany.rocksdb.POJO.ChunkFile;
import com.mycompany.rocksdb.POJO.FileMetadata;
import com.mycompany.rocksdb.POJO.Inode;
import com.mycompany.rocksdb.POJO.LatestIndexMetadata;
import com.mycompany.rocksdb.POJO.VersionIndexMetadata;
import com.mycompany.rocksdb.netserver.Nfsv3Server;
import com.mycompany.rocksdb.storage.DFSInodeReadStream;
import io.reactivex.Flowable;
import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.disposables.Disposable;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.streams.WriteStream;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.net.NetServer;
import io.vertx.reactivex.core.net.NetSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mycompany.rocksdb.myrocksdb.MyRocksDB;
import com.mycompany.rocksdb.utils.MetaKeyUtils;
import reactor.core.publisher.MonoProcessor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.mycompany.rocksdb.MD5Util.calculateMD5;
import static com.mycompany.rocksdb.constant.GlobalConstant.ROCKS_CHUNK_FILE_KEY;
import static com.mycompany.rocksdb.myrocksdb.MyRocksDB.deleteAllMetadata;
import static com.mycompany.rocksdb.netserver.Nfsv3Server.*;


public class FTPServer extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(FTPServer.class);
    static int ftpControllerPort = 2121;
    static int ftpsControllerPort = 990;
    private static String HOST = "0.0.0.0"; // 明确绑定到IPv4本地回环地址
    private static final int PORT = 2121; // <--- 你的服务器端口

    public static String EXTERNAL_IP = null; // 用于NAT环境下的外部IP地址

    static String PRIVATE_PEM = "private.key";
    static String CERT_CRT = "certificate.crt";

    // A container to hold all our subscriptions, so we can clean them up when the verticle stops.
    private final CompositeDisposable disposables = new CompositeDisposable();

    private static final Map<String, List<FsEntry>> virtualFileSystem = new ConcurrentHashMap<>();
    private static final Map<String, Buffer> fileContents = new ConcurrentHashMap<>();

    static {
        virtualFileSystem.computeIfAbsent("/", b -> new ArrayList<>());
        virtualFileSystem.computeIfAbsent("\\", b -> new ArrayList<>());

    }

    @Override
    public void start(Future<Void> startPromise) throws Exception {
        // 控制面
        // 创建 NetServerOptions
        NetServerOptions serverOptions = new NetServerOptions()
                .setReuseAddress(true)
                .setReusePort(true)
                .setSsl(false)
                .setHost(HOST)
                .setPort(PORT)
                .setPemKeyCertOptions(new PemKeyCertOptions().setKeyPath(PRIVATE_PEM)
                        .setCertPath(CERT_CRT));

        // 创建 reactive NetServer
        NetServer server = vertx.createNetServer(serverOptions);

        // 使用 connectStream() 将连接事件转换为一个 Flowable 流
        // 每个从流中发出的事件都是一个新的 NetSocket 连接
        Disposable serverSubscription = server.connectStream()
                .toFlowable()
                .subscribe(
                        this::handleNewConnection, // 为每个新连接调用此方法
                        error -> log.error("Server connection error", error) // 处理服务器级别的错误
                );

        // 将服务器的订阅添加到 disposables 中进行管理
        disposables.add(serverSubscription);

        // 启动服务器并监听端口
        // rxListen() 返回一个 Single，表示启动操作是异步的
        server.rxListen(PORT)
                .subscribe(
                        s -> {
                            log.info("Server started on host " + HOST + " and port " + s.actualPort());
                            startPromise.complete(); // 启动成功
                        },
                        error -> {
                            log.error("Failed to start server", error);
                            startPromise.fail(error); // 启动失败
                        }
                );

        // 数据面
//        serverOptions = new NetServerOptions()
//                .setReuseAddress(true)
//                .setReusePort(true)
//                .setIdleTimeout(10);
//
//        for (int port = FTP_DATA_MIN_PORT; port <= FTP_DATA_MAX_PORT; port++) {
//            NetServer ftpData = vertx.createNetServer(serverOptions);
//            int finalPort = port;
//            Disposable ftpDataSubscription = ftpData.connectStream()
//                    .toFlowable()
//                    .subscribe(socket -> {
//                        DataTransferHandler handler = new DataTransferHandler();
//                        handler.handle(finalPort, socket);
//                    });
//
//            server.rxListen(port)
//                    .subscribe(
//                            s -> {
//                                log.info("Server started on host " + HOST + " and port " + s.actualPort());
//                                startPromise.complete(); // 启动成功
//                            },
//                            error -> {
//                                log.error("Failed to start server", error);
//                                startPromise.fail(error); // 启动失败
//                            }
//                    );
//            disposables.add(ftpDataSubscription);
//        }
    }

    private String formatEntry(FsEntry entry) {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMM dd HH:mm", Locale.ENGLISH);
        String timestamp = now.format(formatter);

        if (entry.isDirectory) {
            return String.format("drwxr-xr-x    2 ftp      ftp          %10d %s %s", entry.size, timestamp, entry.name);
        } else {
            return String.format("-rw-r--r--    1 ftp      ftp      %10d %s %s", entry.size, timestamp, entry.name);
        }
    }

    private void handleNewConnection(NetSocket socket) {
        log.info("Client connected: " + socket.remoteAddress());

        // 1. FTP协议规定，连接成功后服务器必须先发送欢迎消息 (220)
        socket.write("220 Welcome to Vert.x Reactive FTP Server\r\n");

        // 为这个新连接创建一个独立的状态对象
        final FtpConnectionState connectionState = new FtpConnectionState();

        // 2. 创建命令流，使用我们新的 FtpCommandFramer
        Flowable<String> commandStream = socket.toFlowable()
                .compose(new FtpCommandFramer());

        // 3. 订阅命令流并处理 FTP 逻辑
        Disposable socketSubscription = commandStream.subscribe(
                // onNext: 接收到一条完整的 FTP 命令
                commandLine -> handleFtpCommand(commandLine, socket, connectionState),
                // onError: 连接出错
                error -> {
                    log.error("Error on connection [" + socket.remoteAddress() + "]: ", error);
                    socket.close();
                },
                // onComplete: 客户端断开
                () -> log.info("Client disconnected: " + socket.remoteAddress())
        );

        disposables.add(socketSubscription);
    }


    /**
     * 处理具体的 FTP 命令 (这是一个简化的实现)
     * @param commandLine 完整的命令，例如 "USER anonymous"
     * @param socket 用于发送响应
     * @param state 状态
     */
    private void handleFtpCommand(String commandLine, NetSocket socket, FtpConnectionState state) {
        String[] parts = commandLine.split(" ", 2);
        String command = parts[0].toUpperCase();
        String args = parts.length > 1 ? parts[1] : "";

        switch (command) {
            case "AUTH": // <<<<<<<<<< 新增
                log.info("Client requested AUTH with mechanism: " + args + ". This is not supported.");
                // 明确告诉客户端我们不支持加密
                socket.write("504 Command not implemented for that parameter.\r\n");
                break;
            case "USER":
                log.info("USER command with args: " + args);
                // 简单地回复要求输入密码
                socket.write("331 User name okay, need password.\r\n");
                break;
            case "PASS":
                log.info("PASS command with args: ******");
                // 简单地回复登录成功
                socket.write("230 User logged in, proceed.\r\n");
                break;
            case "SYST":
                socket.write("215 UNIX Type: L8\r\n");
                break;
            case "QUIT":
                socket.write("221 Goodbye.\r\n");
                socket.close();
                break;
            case "PASV":
                // 处理被动模式是复杂的，这里只给一个占位符
                handlePasv(socket, state);
                break;
            case "PWD":
                socket.write("257 \"" + state.currentDirectory + "\" is the current directory.\r\n");
                break;
            case "TYPE": // <<<<<<<<<< 新增: 处理传输类型命令
                log.info("Client requested transfer type: " + args);
                // 我们总是以二进制模式工作，所以只需回复OK即可
                // 回复 "200 Type set to I." 是一个更标准的响应
                socket.write("200 Type set to " + args + ".\r\n");
                break;
            case "PORT": // <<<<<<<<<< 新增: 处理 PORT 命令
                handlePort(socket, state, args);
                break;
            case "EPSV": // <<<<<<<<<< 新增
                handleEpsv(socket, state);
                break;
            case "LIST":
                handleList(socket, state);
                break;
            case "CWD":
                handleCwd(socket, state, args);
                break;
            case "RETR": // <<<<<<<<<< 新增
                handleRetr(socket, state, args);
                break;
            case "MKD": // <<<<<<<<<< 新增
                handleMkd(socket, state, args);
                break;
            case "STOR": // <<<<<<<<<< 新增
                handleStor(socket, state, args);
                break;
            case "DELE": // <<<<<<<<<< 新增
                try {
                    handleDELE(socket, state, args);
                } catch (IOException | org.rocksdb.RocksDBException e) {
                    log.error("Error handling DELE command for file: " + args, e);
                    socket.write("451 Requested action aborted. Local error in processing.\r\n");
                }
                break;
            case "REST": // <<<<<<<<<< 新增: 处理断点续传
                handleRest(socket, state, args);
                break;
            case "APPE": // <<<<<<<<<< 新增: 处理文件追加
                handleStor(socket, state, args);
                break;
            case "SIZE": // <<<<<<<<<< 新增: 处理 SIZE 命令
                handleSize(socket, state, args);
                break;
            case "MDTM": // <<<<<<<<<< 新增: 处理 MDTM 命令
                handleMdtm(socket, state, args);
                break;
            case "RNFR": // <<<<<<<<<< 新增: 处理 MDTM 命令
                handleRnfr(socket, state, args);
                break;
            case "RNTO": // <<<<<<<<<< 新增: 处理 MDTM 命令
                handleRnto(socket, state, args);
                break;
            default:
                log.warn("Unknown command: " + command);
                socket.write("500 Unknown command.\r\n");
                break;
        }
    }

    private void sendResponse(NetSocket socket, String response) {
        log.info("Sending response: " + response);
        socket.write(response + "\r\n");
    }

    /**
     * 内部类，用于维护每个客户端连接的状态。
     * 这对于处理需要多个步骤的FTP命令（如PASV后跟LIST）至关重要。
     */
    private static class FtpConnectionState {
        // NetServer dataServer; // 用于PASV模式的数据服务器
        // NetSocket dataSocket; // 建立的数据通道连接
        // 可以在这里添加更多状态，例如当前目录、登录状态等
        // Runnable pendingDataCommand; // <<<<<<<<<< 新增: 用于保存待处理的数据命令
        // 这个 Promise 是我们所有数据通道状态的唯一来源
        Future<NetSocket> dataSocketFuture;
        String currentDirectory = "/"; // <<<<<< 新增：初始化当前目录为根目录
        // int dataPort; // <<<<<< 新增: 用于存储数据端口，用于端口匹配验证
        long restartOffset = 0; // <<<<<< 新增: 用于断点续传的偏移量
        String renameFromPath = null; // 用于存储 RNFR 命令提供的旧路径

        String bucket = "ftp_bucket";

        String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);
    }

    // <<<<<< 新增: FsEntry 内部类，用于表示文件或目录条目 >>>>>>>>
    private static class FsEntry {
        String name;
        boolean isDirectory;
        long size; // 对于文件，表示文件大小；对于目录，通常为 0 或 4096 (Linux习惯)

        public FsEntry(String name, boolean isDirectory, long size) {
            this.name = name;
            this.isDirectory = isDirectory;
            this.size = size;
        }
    }

    /**
     * 处理 LIST 命令: 通过已建立的数据通道发送目录列表。
     */
    private void handleList(NetSocket controlSocket, FtpConnectionState state) {
        log.info("进入 handleList 方法");
        if (state.dataSocketFuture == null) {
            controlSocket.write("425 Use PASV or PORT first.\r\n");
            return;
        }
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMM dd HH:mm", Locale.ENGLISH);
        String timestamp = now.format(formatter);
        log.info("timestamp: {}", timestamp);
        Future<NetSocket> dataSocketFuture = state.dataSocketFuture;
        controlSocket.write("150 Here comes the directory listing.\r\n");

        String bucket = "ftp_bucket";
        String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);

        dataSocketFuture.setHandler(ar -> {
            state.dataSocketFuture = null;

            if (ar.succeeded()) {
                NetSocket dataSocket = ar.result();

                // <<<<<<<<<< 新增: 数据端口验证 >>>>>>>>>>>>
                // if (state.dataPort != 0 && dataSocket.remoteAddress().port() != state.dataPort) {
                //     log.warn("Data connection port mismatch. Expected: {}, Actual: {}", state.dataPort, dataSocket.remoteAddress().port());
                //     controlSocket.write("425 Can't open data connection. Port mismatch.\r\n");
                //     dataSocket.close();
                //     state.dataPort = 0; // 重置数据端口
                //     return;
                // }
                // <<<<<<<<<< 新增结束 >>>>>>>>>>>>

                // <<<<<<<<<< 核心修改从这里开始 >>>>>>>>>>>>.
                // 使用 Path 来安全地解析路径，它能正确处理 ".."
                Path currentPath = Paths.get(state.currentDirectory);

                String currentPathString = currentPath.toString().replace('\\', '/'); // 确保是 UNIX 风格的斜杠
                if (currentPathString.isEmpty()) { // normalize("/") 可能会变为空，需要处理
                    currentPathString = "/";
                }

                // 1. 获取RocksDB中实际的文件和目录条目
                List<LatestIndexMetadata> latestIndexMetadataList = MyRocksDB.listDirectoryContents(targetVnodeId, bucket, currentPathString);

                // 2. 创建一个新的列表，用于最终输出
                List<String> finalEntries = new ArrayList<>();

                // 3. 添加 "." (当前目录)
                finalEntries.add(formatEntry(new FsEntry(".", true, 4096L))); // Use 4096L for long

                // 4. 如果不在根目录，添加 ".." (上级目录)
                if (!state.currentDirectory.equals("/")) {
                    finalEntries.add(formatEntry(new FsEntry("..", true, 4096L))); // Use 4096L for long
                }

                // 5. 将真实的条目添加进来
                for (LatestIndexMetadata latestIndexMetadata : latestIndexMetadataList) {
                    Optional<Inode> inodeOptional = MyRocksDB.getINodeMetaData(targetVnodeId, bucket, latestIndexMetadata.getInode());
                    if (inodeOptional.isPresent()) {
                        Inode inode = inodeOptional.get();
                        boolean isDirectory = inode.getMode() == 16893; // S_IFDIR constant for directory (040755 in octal)
                        long size = isDirectory ? 4096L : inode.getSize();
                        finalEntries.add(formatEntry(new FsEntry(inode.getObjName().substring(inode.getObjName().lastIndexOf('/') + 1), isDirectory, size)));
                    }
                }

                // 6. 将最终的列表转换成要发送的字符串
                String directoryListing = String.join("\r\n", finalEntries);
                if (!directoryListing.isEmpty()) {
                    directoryListing += "\r\n";
                }
                // <<<<<<<<<< 修改结束 >>>>>>>>>>>>

                dataSocket.rxWrite(directoryListing).subscribe(
                        () -> {
                            log.info("Directory listing sent successfully for path: " + state.currentDirectory);
                            dataSocket.close();
                            controlSocket.write("226 Directory send OK.\r\n");
                        },
                        error -> {
                            log.error("Failed to write to data channel", error);
                            dataSocket.close();
                            controlSocket.write("426 Connection closed; transfer aborted.\r\n");
                        }
                );
            } else {
                Throwable error = ar.cause();
                log.error("Data socket future failed", error);
                controlSocket.write("425 Can't open data connection.\r\n");
            }
        });
    }

    /**
     * 处理 EPSV 命令: 这是 PASV 的 IPv6 兼容版本。
     */
    private void handleEpsv(NetSocket controlSocket, FtpConnectionState state) {

        // 清理任何先前的 botched 数据连接状态
        if (state.dataSocketFuture != null && !state.dataSocketFuture.isComplete()) {
            log.warn("Cancelling old dataSocketFuture for EPSV command.");
            state.dataSocketFuture.fail("New EPSV command received, old data connection cancelled.");
        }
        state.dataSocketFuture = null;
        // state.dataPort = 0; // 清理旧的数据端口

        // <<<< CHANGE: 使用 Future.future() 工厂方法 >>>>
        // 这个方法接受一个 handler，该 handler 会收到一个 Promise
        // 我们的任务就是在这个 handler 内部去完成或失败这个 promise
        state.dataSocketFuture = Future.future(promise -> {
            NetServer dataServer = vertx.createNetServer();

            dataServer.connectHandler(dataSocket -> {
                log.info("Client connected to EPSV data channel: " + dataSocket.remoteAddress());
                log.info("Data channel (EPSV) connection established.");
                log.info("Client " + dataSocket.remoteAddress() + " connected to FTP data channel (EPSV).");
                promise.complete(dataSocket); // 当连接到达时，兑现承诺
                //dataServer.close();
                // 数据连接建立后，执行待处理的数据命令
//                if (state.pendingDataCommand != null) {
//                    state.pendingDataCommand.run();
//                    state.pendingDataCommand = null;
//                }
            });

            // 将监听逻辑也放在这个代码块内部
            dataServer.rxListen(0, HOST).subscribe(
                    server -> {
                        int port = server.actualPort();
                        // state.dataPort = port; // 存储实际监听的数据端口
                        log.info("EPSV data server listening on: " + HOST + ":" + port);
                        String response = "229 Entering Extended Passive Mode (|||" + port + "|).\r\n";
                        // 只有在服务器成功监听后才发送响应
                        controlSocket.write(response);
                    },
                    error -> {
                        log.error("Failed to start data server for EPSV", error);
                        controlSocket.write("425 Can't open data connection.\r\n");
                        promise.fail(error); // 如果监听失败，让承诺也失败
                    }
            );
        });
    }

    /**
     * 处理 PASV 命令: 创建一个新的数据服务器并等待连接。
     */
    private void handlePasv(NetSocket controlSocket, FtpConnectionState state) {

        // 清理任何先前的 botched 数据连接状态
        if (state.dataSocketFuture != null && !state.dataSocketFuture.isComplete()) {
            log.warn("Cancelling old dataSocketFuture for PASV command.");
            state.dataSocketFuture.fail("New PASV command received, old data connection cancelled.");
        }
        state.dataSocketFuture = null;
        // state.dataPort = 0; // 清理旧的数据端口

        state.dataSocketFuture = Future.future(promise -> {
            NetServer dataServer = vertx.createNetServer();
            dataServer.connectHandler(dataSocket -> {
                log.info("Client connected to PASV data channel: " + dataSocket.remoteAddress());
                log.info("Data channel (PASV) connection established.");
                log.info("Client " + dataSocket.remoteAddress() + " connected to FTP data channel (PASV).");
                // Add a handler to read data sent by the client
                // dataSocket.handler(buffer -> {
                //     log.info("Received data from client on data channel: " + buffer.toString());
                // });
                promise.complete(dataSocket);
                //dataServer.close();
                // 数据连接建立后，执行待处理的数据命令
//                if (state.pendingDataCommand != null) {
//                    state.pendingDataCommand.run();
//                    state.pendingDataCommand = null;
//                }
            });

            dataServer.rxListen(0, HOST).subscribe(
                    server -> {
                        int port = server.actualPort();
                        log.info("PASV data server listening on: " + controlSocket.localAddress().host() + ":" + port);
                        String usedIp = EXTERNAL_IP != null ? EXTERNAL_IP : controlSocket.localAddress().host();
                        log.info("PASV response IP: " + usedIp + ", Port: " + port);
                        String hostIpFormatted = usedIp.replace('.', ',');
                        String portStr = (port / 256) + "," + (port % 256);
                        String response = "227 Entering Passive Mode (" + hostIpFormatted + "," + portStr + ").\r\n";
                        log.info("PASV: {}", response);
                        controlSocket.write(response);
                    },
                    error -> {
                        log.error("Failed to start data server for PASV", error);
                        controlSocket.write("425 Can't open data connection.\r\n");
                        promise.fail(error);
                    }
            );
        });
    }

    private void handleCwd(NetSocket controlSocket, FtpConnectionState state, String newDir) {
        // 使用 Path 来安全地解析路径，它能正确处理 ".."
        Path currentPath = Paths.get(state.currentDirectory);
        Path newPath = currentPath.resolve(newDir).normalize();

        String newPathString = newPath.toString().replace('\\', '/'); // 确保是 UNIX 风格的斜杠
        if (newPathString.isEmpty()) { // normalize("/") 可能会变为空，需要处理
            newPathString = "/";
        }

        String bucket = "ftp_bucket";
        String targetBucketVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);

        // --- 虚拟文件系统验证 ---
        // 在我们简单的服务器中，只有根目录 "/" 和 "/public_folder" 是有效的
        // <<<<<< 核心修改: 检查 Map 中是否存在该 Key >>>>>>>>
        if ("/".equals(newPathString)
                || newPathString.equals(currentPath.toString())
                || MyRocksDB.getIndexMetaData(targetBucketVnodeId, bucket, newPathString).isPresent()) {
            state.currentDirectory = newPathString;
            log.info("Directory changed to: " + state.currentDirectory);
            controlSocket.write("250 Directory successfully changed.\r\n");
        } else {
            log.warn("Client tried to CWD to an invalid directory: " + newPathString);
            controlSocket.write("550 Requested action not taken. File unavailable.\r\n");
        }
    }

    public static List<byte[]> readSegment(List<Long> offsets, List<Long> lens, long size) throws IOException {
        if (!Paths.get(MyRocksDB.FILE_DATA_DEVICE_PATH).toFile().exists()) {
            log.error("No such path exists...");
            throw new IOException("No such path exists...");
        }

        List<byte[]> result = new ArrayList<>();
        AtomicLong accumulator = new AtomicLong(0);

        for (int i = 0; i < offsets.size(); i++) {
            long offset = offsets.get(i);
            long length = lens.get(i);

            try(FileChannel fileChannel = FileChannel.open(Paths.get(MyRocksDB.FILE_DATA_DEVICE_PATH), StandardOpenOption.READ)) {
                int allocatedLength = (int)length;
                if (accumulator.addAndGet(length) > size) {
                    allocatedLength = (int)(length - accumulator.get() + size);
                }
                ByteBuffer buffer = ByteBuffer.allocate(allocatedLength);
                fileChannel.read(buffer, offset);
                buffer.flip();
                byte[] data = new byte[buffer.remaining()];
                buffer.get(data);

                result.add(data);
            }
        }

        return result;
    }

    private void handleRetr(NetSocket controlSocket, FtpConnectionState state, String filename) {
        if (state.dataSocketFuture == null) {
            controlSocket.write("425 Use PASV or PORT first.\r\n");
            return;
        }

        // --- 虚拟文件系统: 检查文件是否存在 ---
        // 规范化文件路径，以便在虚拟文件系统中查找
        Path requestedPath = Paths.get(state.currentDirectory).resolve(filename).normalize();
        String requestedPathString = requestedPath.toString().replace('\\', '/');

        // Instead of checking fileContents directly, we should check MyRocksDB first
        String bucket = "ftp_bucket";
        String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);

        Optional<VersionIndexMetadata> versionIndexMetadataOptinal = MyRocksDB.getIndexMetaData(targetVnodeId, bucket, requestedPathString);
        if (!versionIndexMetadataOptinal.isPresent()) {
            controlSocket.write("550 Requested action not taken. File unavailable.\r\n");
            state.restartOffset = 0; // Reset offset on file not found
            return;
        }

        state.dataSocketFuture.setHandler(ar -> {
            state.dataSocketFuture = null; // 清除 future 以防万一
            if (ar.succeeded()) {
                NetSocket dataSocket = ar.result();

                controlSocket.write("150 Opening BINARY mode data connection for " + filename + ".\r\n");

                VersionIndexMetadata versionIndexMetadata = versionIndexMetadataOptinal.get();
                // 1. 在控制通道上发送初始响应
                Inode inode = MyRocksDB.getINodeMetaData(targetVnodeId, bucket, versionIndexMetadata.getInode()).get();

                // 2. 创建你的自定义 ReadStream
                DFSInodeReadStream fileStream = new DFSInodeReadStream(vertx.getDelegate(), inode);

                // 3. 使用 Pipe 传输
                // pipeTo 会自动把 fileStream 的数据搬运到 socket
                // 并自动处理 socket.writeQueueFull() 时的 pause/resume
                fileStream.pipeTo(dataSocket.getDelegate(), ar2 -> {
                    if (ar2.succeeded()) {
                        log.info("File '" + filename + "' sent successfully.");

                        // 关闭数据连接
                        // 注意：这里用 rxDataSocket 或 coreDataSocket 关闭都可以
                        dataSocket.close();

                        // 给控制连接发送 FTP 226 响应
                        controlSocket.write("226 Transfer complete.\r\n");

                        // 重置状态
                        if (state != null) {
                            state.restartOffset = 0;
                        }
                    } else {
                        // --- 失败分支 (对应你的第二个 lambda) ---
                        Throwable error = ar.cause(); // 获取具体的异常信息

                        log.error("Failed to write file content to data channel", error);

                        dataSocket.close();

                        // 给控制连接发送 FTP 426 响应
                        controlSocket.write("426 Connection closed; transfer aborted.\r\n");

                        if (state != null) {
                            state.restartOffset = 0;
                        }
                    }
                });

            } else {
                log.error("Data socket future failed for RETR", ar.cause());
                controlSocket.write("425 Can't open data connection.\r\n");
                state.restartOffset = 0; // Reset offset on error
            }
        });
    }

    private void handleMkd(NetSocket controlSocket, FtpConnectionState state, String pathArg) {
        log.info("--- Entering handleMkd ---");
        log.info("Current Directory: '" + state.currentDirectory + "'");
        log.info("Argument (pathArg): '" + pathArg + "'");
        log.info("FileSystem Keys: " + FTPServer.virtualFileSystem.keySet());

        Path newDirPath;
        if (pathArg.startsWith("/")) {
            newDirPath = Paths.get(pathArg).normalize();
        } else {
            newDirPath = Paths.get(state.currentDirectory).resolve(pathArg).normalize();
        }

        String newDirPathString = newDirPath.toString().replace('\\', '/');
        if (newDirPathString.equals("/")) {
            controlSocket.write("550 Requested action not taken. Cannot create root.\r\n");
            return;
        }

        Path parentDirPath = newDirPath.getParent();
        String parentDirPathString;
        // <<<<<<<<<<<< 关键修复：正确处理根目录的父级 >>>>>>>>>>>>>
        if (parentDirPath == null || parentDirPath.toString().equals("/") || parentDirPath.toString().equals("\\")) {
            parentDirPathString = "/";
        } else {
            parentDirPathString = parentDirPath.toString().replace('\\', '/');
        }

        log.info("Resolved New Path: '" + newDirPathString + "'");
        log.info("Resolved Parent Path: '" + parentDirPathString + "'");

        String bucket = "ftp_bucket"; // Placeholder
        String object = newDirPathString; // Use the full path as object name
        String contentType = "directory"; // Content type for directories
        String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);

        // Save inode metadata for the new directory
        Optional<Inode> inodeOptional = MyRocksDB.saveIndexMetaAndInodeData(
                targetVnodeId, bucket, object, 0, contentType, 16893);

        if (inodeOptional.isPresent()) {
            log.info("Directory created and metadata saved to RocksDB: " + newDirPathString);
            controlSocket.write("257 \"" + newDirPathString + "\" created.\r\n");
        } else {
            log.error("Failed to save inode metadata for directory: " + newDirPathString);
            controlSocket.write("451 Requested action aborted. Local error in processing.\r\n");
        }
    }

    private final int MAX_FILE_SIZE = 4 * 1024 * 1024;

    private void handleStor(NetSocket controlSocket, FtpConnectionState state, String filename) {
        log.info("--- Entering handleStor ---");
        log.info("Current Directory: '" + state.currentDirectory + "'");
        log.info("Argument (filename): '" + filename + "'");
        log.info("FileSystem Keys: " + FTPServer.virtualFileSystem.keySet());

        if (state.dataSocketFuture == null) {
            controlSocket.write("425 Use PASV or PORT first.\r\n");
            return;
        }

        Path newFilePath;
        if (filename.startsWith("/")) {
            newFilePath = Paths.get(filename).normalize();
        } else {
            newFilePath = Paths.get(state.currentDirectory).resolve(filename).normalize();
        }

        String newFilePathString = newFilePath.toString().replace('\\', '/');
        if (newFilePathString.equals("/")) {
            controlSocket.write("550 Requested action not taken. Cannot create root.\r\n");
            return;
        }

        Path parentDirPath = newFilePath.getParent();
        String parentDirPathString;
        // <<<<<<<<<<<< 关键修复：正确处理根目录的父级 >>>>>>>>>>>>>
        if (parentDirPath == null || parentDirPath.toString().equals("/") || parentDirPath.toString().equals("\\")) {
            parentDirPathString = "/";
        } else {
            parentDirPathString = parentDirPath.toString().replace('\\', '/');
        }

        log.info("Resolved New Path: '" + newFilePathString + "'");
        log.info("Resolved Parent Path: '" + parentDirPathString + "'");

        final String objName = newFilePath.toString().replace('\\', '/');
        String bucket = "ftp_bucket";
        String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);
        String contentType = "application/octet-stream";

        Optional<Inode> inodeOptional = MyRocksDB.saveIndexMetaAndInodeData(
                targetVnodeId, bucket, objName, 0, contentType, 12345
        );

        final long inodeId = inodeOptional.get().getInodeId();

        state.dataSocketFuture.setHandler(ar -> {
            state.dataSocketFuture = null; // 清除 future 以防万一
            if (ar.succeeded()) {
                NetSocket dataSocket = ar.result();

                controlSocket.write("150 File status okay; about to open data connection.\r\n");

                // ... 环境准备 ...
                long startPosition = (state != null) ? state.restartOffset : 0L;
                AtomicLong offset = new AtomicLong(startPosition);
                int MAX_CONCURRENT_UPLOADS = 2; // 流控阈值：最多允许 2 个分片同时在后台上传
                AtomicInteger activeUploads = new AtomicInteger(0); // 当前正在上传的任务数
                AtomicLong size = new AtomicLong(0);
                AtomicLong currentOffset = new AtomicLong(offset.get());
                AtomicReference<PutObjectTask> currentTaskRef = new AtomicReference<>();
                AtomicBoolean hasFailed = new AtomicBoolean(false); // 快速失败标志
                // 1. 创建 suc 对象 (Subject)
                MonoProcessor<Boolean> suc = MonoProcessor.create();

                // 辅助方法：创建新任务
                Runnable createNewTask = () -> {
                    PutObjectTask newTask = new PutObjectTask(state.bucket, objName, inodeId, currentOffset.get());

                    // 【关键流控逻辑 1】：设置任务完成的回调
                    newTask.setOnUploadFinished(() -> {
                        int currentCount = activeUploads.decrementAndGet();
                        // 如果积压的任务处理完了，并且 Socket 是暂停状态，则恢复读取
                        if (currentCount < MAX_CONCURRENT_UPLOADS && !hasFailed.get()) {
                            // 注意：Vert.x 的 handler 必须在 EventLoop 线程执行，如果 uploadToStorage 是在其他线程回调，这里需要 runOnContext
                            // vertx.getOrCreateContext().runOnContext(v -> socket.resume());
                            try {
                                dataSocket.resume();
                            } catch (Exception e) {
                                // 忽略 socket 已经关闭的情况
                            }
                        }
                    });

                    currentTaskRef.set(newTask);
                };

                // 初始化第一个任务
                createNewTask.run();
                currentTaskRef.get().request();

                dataSocket.handler(buffer -> {
                    // 如果已经失败，不再处理后续数据
                    if (hasFailed.get()) {
                        dataSocket.pause();
                        return;
                    }

                    PutObjectTask task = currentTaskRef.get();

                    // 1. 检查是否需要切片
                    if (size.get() >= MAX_FILE_SIZE) {
                        // 提交旧任务
                        task.complete();

                        // 【关键流控逻辑 2】：增加计数并检查是否暂停
                        int pendingCount = activeUploads.incrementAndGet();
                        if (pendingCount >= MAX_CONCURRENT_UPLOADS) {
                            // 后台积压任务太多，暂停接收网络数据
                            dataSocket.pause();
                        }

                        // 订阅结果（错误处理）
                        task.res.subscribe(success -> {
                            if (!success && hasFailed.compareAndSet(false, true)) {
                                suc.onNext(false);
                                dataSocket.close(); // 失败直接断开
                            }
                        });

                        // 更新 Offset，重置 Size
                        currentOffset.addAndGet(size.get());
                        size.set(0L);

                        // 创建新任务
                        createNewTask.run();
                        task = currentTaskRef.get(); // 切换引用
                    }

                    // 2. 写入数据
                    task.handle(buffer);
                    size.addAndGet(buffer.length());
                });

                log.info("DataSocket handler registered for file " + filename);

                dataSocket.endHandler(v -> {
                    if (hasFailed.get()) return;
                    try {
                        PutObjectTask lastTask = currentTaskRef.get();
                        lastTask.complete();
                        activeUploads.incrementAndGet(); // 最后一个任务也算入计数，虽然此时 pause 也没用了

                        // 这里需要等待所有任务完成，参考之前的 CompositeFuture 逻辑
                        // ...
                        lastTask.res.subscribe(suc::onNext);

                        suc.subscribe(success -> {
                            dataSocket.close();
                            controlSocket.write("226 Transfer complete.\r\n");
                        });

                        //state.restartOffset = 0; // Reset offset after successful transfer

                    } catch (Exception e) {
                        log.error("Failed to save file to RocksDB: " + filename, e);
                        dataSocket.close();
                        controlSocket.write("426 Connection closed; transfer     aborted.\r\n");
                        state.restartOffset = 0; // Reset offset on error
                    }
                });

                dataSocket.exceptionHandler(error -> {
                    log.error("Error on data channel for STOR of file " + filename, error);
                    dataSocket.close();
                    controlSocket.write("426 Connection closed; transfer aborted.\r\n");
                    state.restartOffset = 0; // Reset offset on error
                });

            } else {
                log.error("Data socket future failed for STOR", ar.cause());
                controlSocket.write("425 Can't open data connection.\r\n");
                state.restartOffset = 0; // Reset offset on error
            }
        });
    }

    private void handleDELE(NetSocket socket, FtpConnectionState state, String fileName) throws IOException, org.rocksdb.RocksDBException {
        // Construct the full path of the file
        Path filePath = Paths.get(state.currentDirectory).resolve(fileName).normalize();
        String fullPath = filePath.toString().replace('\\', '/');

        String bucket = "ftp_bucket";
        String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);

        try {
            VersionIndexMetadata versionIndexMetadata = MyRocksDB.getIndexMetaData(targetVnodeId, bucket, fullPath).orElseThrow(() -> new org.rocksdb.RocksDBException("Could not find index metadata for " + bucket));

            long inodeId = versionIndexMetadata.getInode();
            Inode inode = MyRocksDB.getINodeMetaData(targetVnodeId, bucket, inodeId).orElseThrow(() -> new org.rocksdb.RocksDBException("Could not find Inode for " + bucket));

            List<Inode.InodeData> list = inode.getInodeData();

            for (Inode.InodeData inodeData : list) {
                if (inodeData.fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                    String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(inode.getBucket(), inodeData.fileName);
                    ChunkFile chunkFile = MyRocksDB.getChunkFileMetaData(chunkKey).orElseThrow(() -> new org.rocksdb.RocksDBException("Could not find chunkFile for " + chunkKey));

                    List<Inode.InodeData> chunkList = new ArrayList<>(); // chunkFile.getChunkList();

                    for (Inode.InodeData realData : chunkList) {
                        MyRocksDB.deleteFile(realData.fileName);
                    }

                    MyRocksDB.deleteChunkFileMetaData(chunkKey);
                }
            }

            // Call deleteAllMetadata to delete all associated metadata
            log.info("Stamp {} for deleting file: {}", versionIndexMetadata.getStamp(), fileName);
            deleteAllMetadata(targetVnodeId, bucket, inode.getObjName(), versionIndexMetadata.getStamp());

            sendResponse(socket, "250 Requested file action okay, completed.");

        } catch (org.rocksdb.RocksDBException e) {
            log.error("Failed to delete file from RocksDB: " + fileName, e);
            sendResponse(socket, "550 Could not delete file (RocksDB error).");
        } catch (IOException e) {
            log.error("IO error during file deletion: " + fileName, e);
            sendResponse(socket, "550 Could not delete file (IO error).");
        }
    }

    private void handleRest(NetSocket controlSocket, FtpConnectionState state, String offsetString) {
        try {
            long offset = Long.parseLong(offsetString);
            if (offset >= 0) {
                state.restartOffset = offset;
                controlSocket.write("350 Restarting at " + offset + ". Send STORE or RETRIEVE to resume.\r\n");
            } else {
                controlSocket.write("501 Syntax error in parameters or arguments.\r\n");
            }
        } catch (NumberFormatException e) {
            controlSocket.write("501 Syntax error in parameters or arguments.\r\n");
        }
    }

    private void handleAppe(NetSocket controlSocket, FtpConnectionState state, String filename) {
        // APPE is similar to STOR but appends to existing file.
        // We need to get the current size of the file and set the restartOffset to that size.
        Path filePath = Paths.get(state.currentDirectory).resolve(filename).normalize();
        String fullPath = filePath.toString().replace('\\', '/');

        String bucket = "ftp_bucket";
        String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);

        Optional<VersionIndexMetadata> versionIndexMetadataOptinal = MyRocksDB.getIndexMetaData(targetVnodeId, bucket, fullPath);
        if (versionIndexMetadataOptinal.isPresent()) {
            VersionIndexMetadata versionIndexMetadata = versionIndexMetadataOptinal.get();
            Inode inode = MyRocksDB.getINodeMetaData(targetVnodeId, bucket, versionIndexMetadata.getInode()).get();
            long reqOffset = state.restartOffset;

//            state.dataSocketFuture.setHandler(ar -> {
//                state.dataSocketFuture = null; // 清除 future 以防万一
//                if (ar.succeeded()) {
//                    NetSocket dataSocket = ar.result();
//
//                    controlSocket.write("150 File status okay; about to open data connection.\r\n");
//
//                    final Buffer receivedData = Buffer.buffer();
//                    AtomicLong totalPumped = new AtomicLong(0);
//                    dataSocket.handler(buffer -> {
//                        receivedData.appendBuffer(buffer);
//                        totalPumped.addAndGet(buffer.length());
//                    });
//                    log.info("DataSocket handler registered for file " + filename);
//                    dataSocket.resume();
//
//                    dataSocket.endHandler(v -> {
//                        long fileSize = receivedData.length();
//
//                        String object = fullPath; // Use the full path as object name
//                        String contentType = "application/octet-stream"; // Default for binary data
//                        String requestId = MetaKeyUtils.getRequestId();
//
//                        Inode.InodeData inodeData = new Inode.InodeData();
//                        inodeData.offset = reqOffset;
//                        inodeData.size = fileSize;
//                        inodeData.fileName = MetaKeyUtils.getObjFileName(bucket, object, requestId);
//                        inodeData.etag = "calculateMD5(receivedData.getBytes());";
//                        inodeData.storage = "dataa";
//
//                        log.info("DataSocket endHandler registered for file " + filename + ", size: " + fileSize);
//                        try {
//
//                            List<Inode.InodeData> list = inode.getInodeData();
//                            // 如果写入的位置大于文件的末端
//                            if (reqOffset >= inode.getSize()) {
//                                if (reqOffset > inode.getSize()) {
//                                    appendData(list , Inode.InodeData.newHoleFile(reqOffset - inode.getSize()), inode, null);
//                                    appendData(list, inodeData, inode, receivedData.getBytes());
//                                }
//                                // 恰好追加
//                                else {
//                                    appendData(list, inodeData, inode, receivedData.getBytes());
//                                }
//                            }
//                            // 新加入的 inodeata 与 原来的数据有交集
//                            else {
//                                Inode.InodeData last = list.get(list.size() - 1);
//                                String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(inode.getBucket(), last.fileName);
//                                ChunkFile chunkFile = MyRocksDB.getChunkFileMetaData(chunkKey).orElseThrow(() -> new RuntimeException("chunk file not found for chunkKey: " + chunkKey));
//                                long updatedtotalSize = Inode.partialOverwrite3(chunkFile, reqOffset, inodeData);
//
//                                String verisonKey = MetaKeyUtils.getVersionMetaDataKey(targetVnodeId, inode.getBucket(), inode.getObjName(), null);
//
//                                MyRocksDB.saveFileMetaData(inodeData.fileName, verisonKey, receivedData.getBytes(), fileSize, true);
//
//                                chunkFile.setSize(chunkFile.getSize() + updatedtotalSize);
//                                inode.setSize(chunkFile.getSize() + updatedtotalSize);
//                                last.setChunkNum(chunkFile.getChunkList().size());
//                                last.setSize(chunkFile.getSize() + updatedtotalSize);
//
//                                MyRocksDB.saveINodeMetaData(targetVnodeId, inode);
//                                MyRocksDB.saveChunkFileMetaData(chunkKey, chunkFile);
//                            }
//
//                            log.info("File '" + object + "' received successfully. Total size: " + totalPumped + " bytes.");
//                            dataSocket.close();
//                            controlSocket.write("226 Transfer complete.\r\n");
//
//                        } catch (Exception e2) {
//                            log.error("Failed to save file to RocksDB: " + filename, e2);
//                            dataSocket.close();
//                            controlSocket.write("426 Connection closed; transfer aborted.\r\n");
//                            state.restartOffset = 0; // Reset offset on error
//                        }
//                    });
//
//                    dataSocket.exceptionHandler(error -> {
//                        log.error("Error on data channel for STOR of file " + filename, error);
//                        dataSocket.close();
//                        controlSocket.write("426 Connection closed; transfer aborted.\r\n");
//                        state.restartOffset = 0; // Reset offset on error
//                    });

//                } else {
//                    log.error("Data socket future failed for STOR", ar.cause());
//                    controlSocket.write("425 Can't open data connection.\r\n");
//                    state.restartOffset = 0; // Reset offset on error
//                }
//            });

        } else {
            // If file doesn't exist, treat as a normal STOR (start from 0)
            state.restartOffset = 0;
            controlSocket.write("350 Creating new file. Restarting at 0.\r\n");
        }

        // Then proceed with STOR logic, which will use the restartOffset
        //handleStor(controlSocket, state, filename);
    }

    private void handleSize(NetSocket controlSocket, FtpConnectionState state, String filename) {
        Path filePath = Paths.get(state.currentDirectory).resolve(filename).normalize();
        String fullPath = filePath.toString().replace('\\', '/');

        String bucket = "ftp_bucket";
        String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);

        Optional<VersionIndexMetadata> fileMetadataOptional = MyRocksDB.getIndexMetaData(targetVnodeId, bucket, fullPath);
        if (fileMetadataOptional.isPresent()) {
            Inode inode = MyRocksDB.getINodeMetaData(targetVnodeId, bucket, fileMetadataOptional.get().getInode()).get();
            log.info("213 " + inode.getSize() + "\r\n");
            controlSocket.write("213 " + inode.getSize() + "\r\n");
        } else {
            controlSocket.write("550 Could not get file size.\r\n");
        }
    }

    private void handleMdtm(NetSocket controlSocket, FtpConnectionState state, String filename) {
        log.info("--- Entering handleMdtm ---");
        log.info("Current Directory: '" + state.currentDirectory + "'");
        log.info("Argument (filename): '" + filename + "'");
        log.info("FileSystem Keys: " + FTPServer.virtualFileSystem.keySet());

        Path filePath = Paths.get(state.currentDirectory).resolve(filename).normalize();
        String fullPath = filePath.toString().replace('\\', '/');

        String bucket = "ftp_bucket";
        String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);

        Optional<VersionIndexMetadata> versionIndexMetadataOptinal = MyRocksDB.getIndexMetaData(targetVnodeId, bucket, fullPath);
        if (versionIndexMetadataOptinal.isPresent()) {
            VersionIndexMetadata versionIndexMetadata = versionIndexMetadataOptinal.get();
            Inode inode = MyRocksDB.getINodeMetaData(targetVnodeId, bucket, versionIndexMetadata.getInode()).get();
            LocalDateTime lastModified = inode.getLastModified();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMM dd HH:mm", Locale.ENGLISH);
            String lastModifiedString = lastModified.format(formatter);
            controlSocket.write("250 " + lastModifiedString + "\r\n");
        } else {
            controlSocket.write("550 Could not get file last modified time.\r\n");
        }
    }

    private void handlePort(NetSocket controlSocket, FtpConnectionState state, String args) {
        // PORT command arguments are in the format "h1,h2,h3,h4,p1,p2"
        try {
            String[] parts = args.split(",");
            if (parts.length != 6) {
                controlSocket.write("501 Syntax error in parameters or arguments.\r\n");
                return;
            }

            String ipAddress = String.join(".", parts[0], parts[1], parts[2], parts[3]);
            int p1 = Integer.parseInt(parts[4]);
            int p2 = Integer.parseInt(parts[5]);
            int dataPort = (p1 * 256) + p2;

            log.info("Client requested PORT active connection to " + ipAddress + ":" + dataPort);

            // Clear any previous data connection state
            if (state.dataSocketFuture != null && !state.dataSocketFuture.isComplete()) {
                log.warn("Cancelling old dataSocketFuture for PORT command.");
                state.dataSocketFuture.fail("New PORT command received, old data connection cancelled.");
            }
            state.dataSocketFuture = null;

            // Create a new future for the data socket
            state.dataSocketFuture = Future.future(promise -> {
                vertx.createNetClient().rxConnect(dataPort, ipAddress)
                        .subscribe(
                                dataSocket -> {
                                    log.info("Connected to client data channel: " + dataSocket.remoteAddress());
                                    log.info("Data channel (PORT) connection established.");
                                    promise.complete(dataSocket);
                                },
                                error -> {
                                    log.error("Failed to connect to client data channel for PORT: " + ipAddress + ":" + dataPort, error);
                                    controlSocket.write("425 Can't open data connection.\r\n");
                                    promise.fail(error);
                                }
                        );
            });

            controlSocket.write("200 PORT command successful.\r\n");

        } catch (NumberFormatException e) {
            log.error("Number format exception in PORT command arguments: " + args, e);
            controlSocket.write("501 Syntax error in parameters or arguments (numeric expected).\r\n");
        } catch (Exception e) {
            log.error("Error handling PORT command: " + args, e);
            controlSocket.write("500 Internal server error.\r\n");
        }
    }

    private void handleRnfr(NetSocket controlSocket, FtpConnectionState state, String path) {
        Path requestedPath = Paths.get(state.currentDirectory).resolve(path).normalize();
        String fullPath = requestedPath.toString().replace('\\', '/');

        String bucket = "ftp_bucket";
        String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);

        // Check if the file/directory exists
        if (MyRocksDB.getIndexMetaData(targetVnodeId, bucket, fullPath).isPresent()) {
            state.renameFromPath = fullPath;
            controlSocket.write("350 Requested file action pending further information.\r\n");
        } else {
            controlSocket.write("550 Requested action not taken. File unavailable.\r\n");
            state.renameFromPath = null; // Clear if not found
        }
    }

    private void handleRnto(NetSocket controlSocket, FtpConnectionState state, String newPath) {
        if (state.renameFromPath == null) {
            controlSocket.write("503 Bad sequence of commands.\r\n");
            return;
        }

        Path newRequestedPath = Paths.get(state.currentDirectory).resolve(newPath).normalize();
        String newFullPath = newRequestedPath.toString().replace('\\', '/');

        String bucket = "ftp_bucket";
        String targetVnodeId = MetaKeyUtils.getTargetVnodeId(bucket);

        // Check if the destination already exists
        if (MyRocksDB.getIndexMetaData(targetVnodeId, bucket, newFullPath).isPresent()) {
            controlSocket.write("550 Requested action not taken. File already exists.\r\n");
            state.renameFromPath = null; // Clear path
            return;
        }

        try {
            MyRocksDB.renameFile(targetVnodeId, bucket, state.renameFromPath, newFullPath);
            controlSocket.write("250 Requested file action okay, completed.\r\n");
        } catch (org.rocksdb.RocksDBException e) {
            log.error("Failed to rename file from " + state.renameFromPath + " to " + newFullPath, e);
            controlSocket.write("451 Requested action aborted. Local error in processing.\r\n");
        } finally {
            state.renameFromPath = null; // Always clear after attempt
        }
    }

     @Override
    public void stop(Future<Void> stopPromise) throws Exception {
        log.info("Stopping FTP Server Verticle...");
        disposables.clear();
        stopPromise.complete();
    }
}
