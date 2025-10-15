package com.mycompany.rocksdb.ftp;

import com.mycompany.rocksdb.netserver.MountServer;
import com.mycompany.rocksdb.netserver.RpcParseState;
import io.netty.util.concurrent.Promise;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.disposables.Disposable;
import io.vertx.core.Future;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.net.NetServer;
import io.vertx.reactivex.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class FTPServer extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(FTPServer.class);
    static int ftpControllerPort = 2121;
    static int ftpsControllerPort = 990;
    private static String HOST = "0.0.0.0"; // 明确绑定到IPv4本地回环地址
    private static int FTP_DATA_MIN_PORT = 1200;
    private static int FTP_DATA_MAX_PORT = 1210;
    private static final int PORT = 2121; // <--- 你的服务器端口

    public static String EXTERNAL_IP = null; // 用于NAT环境下的外部IP地址

    static String PRIVATE_PEM = "private.key";
    static String CERT_CRT = "certificate.crt";

    private RpcParseState currentState = RpcParseState.READING_MARKER;
    private int expectedFragmentLength;
    private boolean isLastFragment;

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

    // <<<<<< 新增: 一个辅助方法来格式化条目，避免重复代码 >>>>>>>>
    private String formatDirectoryEntry(String name) {
        return "drwxr-xr-x    2 ftp      ftp          4096 Jan 02 13:00 " + name;
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

        connectionState.dataPort = 0; // 初始化数据端口为0

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
            default:
                log.warn("Unknown command: " + command);
                socket.write("500 Unknown command.\r\n");
                break;
        }
    }

    /**
     * 内部类，用于维护每个客户端连接的状态。
     * 这对于处理需要多个步骤的FTP命令（如PASV后跟LIST）至关重要。
     */
    private static class FtpConnectionState {
        NetServer dataServer; // 用于PASV模式的数据服务器
        NetSocket dataSocket; // 建立的数据通道连接
        // 可以在这里添加更多状态，例如当前目录、登录状态等
        Runnable pendingDataCommand; // <<<<<<<<<< 新增: 用于保存待处理的数据命令
        // 这个 Promise 是我们所有数据通道状态的唯一来源
        Future<NetSocket> dataSocketFuture;
        String currentDirectory = "/"; // <<<<<< 新增：初始化当前目录为根目录
        int dataPort; // <<<<<< 新增: 用于存储数据端口，用于端口匹配验证
    }

    // <<<<<< 新增: FsEntry 内部类，用于表示文件或目录条目 >>>>>>>>
    private static class FsEntry {
        String name;
        boolean isDirectory;
        long size; // 对于文件，表示文件大小；对于目录，通常为 0 或 4096 (Linux习惯)

        public FsEntry(String name, boolean isDirectory) {
            this(name, isDirectory, 0); // 默认大小为0
        }

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

        dataSocketFuture.setHandler(ar -> {
            state.dataSocketFuture = null;

            if (ar.succeeded()) {
                NetSocket dataSocket = ar.result();

                // <<<<<<<<<< 核心修改从这里开始 >>>>>>>>>>>>

                // 1. 获取真实的文件和目录条目
                List<FsEntry> realEntries = FTPServer.virtualFileSystem.getOrDefault(state.currentDirectory, new ArrayList<>());

                // 2. 创建一个新的列表，用于最终输出
                List<String> finalEntries = new ArrayList<>();

                // 3. 添加 "." (当前目录)
                finalEntries.add(formatEntry(new FsEntry(".", true, 4096)));

                // 4. 如果不在根目录，添加 ".." (上级目录)
                if (!state.currentDirectory.equals("/")) {
                    finalEntries.add(formatEntry(new FsEntry("..", true, 4096)));
                }

                // 5. 将真实的条目添加进来
                for (FsEntry entry : realEntries) {
                    finalEntries.add(formatEntry(entry));
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
        state.dataPort = 0; // 清理旧的数据端口

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
                if (state.pendingDataCommand != null) {
                    state.pendingDataCommand.run();
                    state.pendingDataCommand = null;
                }
            });

            // 将监听逻辑也放在这个代码块内部
            dataServer.rxListen(0, HOST).subscribe(
                    server -> {
                        int port = server.actualPort();
                        state.dataPort = port; // 存储实际监听的数据端口
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
        state.dataPort = 0; // 清理旧的数据端口

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
                if (state.pendingDataCommand != null) {
                    state.pendingDataCommand.run();
                    state.pendingDataCommand = null;
                }
            });

            dataServer.rxListen(0, HOST).subscribe(
                    server -> {
                        int port = server.actualPort();
                        state.dataPort = port; // 存储实际监听的数据端口
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

        // --- 虚拟文件系统验证 ---
        // 在我们简单的服务器中，只有根目录 "/" 和 "/public_folder" 是有效的
        // <<<<<< 核心修改: 检查 Map 中是否存在该 Key >>>>>>>>
        if (FTPServer.virtualFileSystem.containsKey(newPathString)) {
            state.currentDirectory = newPathString;
            log.info("Directory changed to: " + state.currentDirectory);
            controlSocket.write("250 Directory successfully changed.\r\n");
        } else {
            log.warn("Client tried to CWD to an invalid directory: " + newPathString);
            controlSocket.write("550 Requested action not taken. File unavailable.\r\n");
        }
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

        if (!FTPServer.fileContents.containsKey(requestedPathString)) {
            controlSocket.write("550 Requested action not taken. File unavailable.\r\n");
            return;
        }

        state.pendingDataCommand = () -> {
            state.dataSocketFuture.setHandler(ar -> {
                state.dataSocketFuture = null; // 清除 future 以防万一
                if (ar.succeeded()) {
                    NetSocket dataSocket = ar.result();

                    // 1. 在控制通道上发送初始响应
                    Buffer fileContent = getVirtualFileContent(requestedPathString, state);
                    controlSocket.write("150 Opening BINARY mode data connection for " + filename + " (" + fileContent.length() + " bytes).\r\n");

                    dataSocket.rxWrite(fileContent).subscribe(
                            () -> {
                                log.info("File '" + filename + "' sent successfully.");
                                dataSocket.close();
                                controlSocket.write("226 Transfer complete.\r\n");
                                state.dataPort = 0; // 重置数据端口
                            },
                            error -> {
                                log.error("Failed to write file content to data channel", error);
                                dataSocket.close();
                                controlSocket.write("426 Connection closed; transfer aborted.\r\n");
                                state.dataPort = 0; // 重置数据端口
                            }
                    );
                } else {
                    log.error("Data socket future failed for RETR", ar.cause());
                    controlSocket.write("425 Can't open data connection.\r\n");
                    state.dataPort = 0; // 重置数据端口
                }
            });
        };

        // 如果数据连接已经就绪，立即执行
        if (state.dataSocketFuture != null && state.dataSocketFuture.isComplete() && state.dataSocketFuture.succeeded()) {
            log.info("Data channel already established, executing pending RETR command immediately.");
            state.pendingDataCommand.run();
        } else {
            log.info("Data channel not yet established, RETR command pending.");
        }
    }

    private Buffer getVirtualFileContent(String filename, FtpConnectionState state) {
        // 先尝试从实际存储中获取文件内容
        Buffer contentBuffer = FTPServer.fileContents.get(filename);
        if (contentBuffer != null) {
            return contentBuffer;
        }
        return Buffer.buffer();
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

        if (!FTPServer.virtualFileSystem.containsKey(parentDirPathString)) {
            controlSocket.write("550 Requested action not taken. Parent directory does not exist. ("+parentDirPathString+")\r\n");
            return;
        }

        if (FTPServer.virtualFileSystem.containsKey(newDirPathString)) {
            controlSocket.write("550 Requested action not taken. File exists.\r\n");
            return;
        }

        FTPServer.virtualFileSystem.put(newDirPathString, new ArrayList<>());

        String newDirName = newDirPath.getFileName().toString();
        List<FsEntry> parentDirContent = FTPServer.virtualFileSystem.get(parentDirPathString);
        if (parentDirContent != null) {
            parentDirContent.add(new FsEntry(newDirName, true, 4096)); // 目录默认大小为 4096 字节
        }


        log.info("Directory created: " + newDirPathString);
        controlSocket.write("257 \"" + newDirPathString + "\" created.\r\n");
    }

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

        if (!FTPServer.virtualFileSystem.containsKey(parentDirPathString)) {
            controlSocket.write("550 Requested action not taken. Parent directory does not exist. ("+parentDirPathString+")\r\n");
            return;
        }

        // 如果文件已存在，则允许覆盖，不返回错误
        // if (FTPServer.virtualFileSystem.containsKey(newFilePathString)) {
        //     controlSocket.write("550 Requested action not taken. File exists.\r\n");
        //     return;
        // }

        state.pendingDataCommand = () -> {
            state.dataSocketFuture.setHandler(ar -> {
                state.dataSocketFuture = null; // 清除 future 以防万一
                if (ar.succeeded()) {
                    NetSocket dataSocket = ar.result();

                    // 1. 在控制通道上发送初始响应
                    controlSocket.write("150 File status okay; about to open data connection.\r\n");

                    final Buffer receivedData = Buffer.buffer();
                    dataSocket.handler(buffer -> {
                        //log.info("Received data chunk of size: " + buffer.length() + " bytes for file " + filename);
                        //log.info("Data chunk content: " + buffer.toString(java.nio.charset.StandardCharsets.UTF_8));
                        receivedData.appendBuffer(buffer);
                    });
                    log.info("DataSocket handler registered for file " + filename);
                    dataSocket.resume();

                    dataSocket.endHandler(v -> {
                        log.info("DataSocket endHandler registered for file " + filename);
                        long fileSize = receivedData.length();

                        List<FsEntry> parentDirContent = FTPServer.virtualFileSystem.get(parentDirPathString);
                        if (parentDirContent != null) {
                            // 检查文件是否已存在
                            boolean found = false;
                            for (int i = 0; i < parentDirContent.size(); i++) {
                                if (parentDirContent.get(i).name.equals(newFilePath.getFileName().toString())) {
                                    parentDirContent.set(i, new FsEntry(newFilePath.getFileName().toString(), false, fileSize));
                                    found = true;
                                    break;
                                }
                            }
                            if (!found) {
                                parentDirContent.add(new FsEntry(newFilePath.getFileName().toString(), false, fileSize));
                            }
                        }

                        FTPServer.fileContents.put(newFilePathString, receivedData);

                        log.info("File '" + filename + "' received successfully. Total size: " + fileSize + " bytes.");
                        dataSocket.close();
                        controlSocket.write("226 Transfer complete.\r\n");
                        state.dataPort = 0; // 重置数据端口
                    });

                    dataSocket.exceptionHandler(error -> {
                        log.error("Error on data channel for STOR of file " + filename, error);
                        dataSocket.close();
                        controlSocket.write("426 Connection closed; transfer aborted.\r\n");
                        state.dataPort = 0; // 重置数据端口
                    });

                } else {
                    log.error("Data socket future failed for STOR", ar.cause());
                    controlSocket.write("425 Can't open data connection.\r\n");
                    state.dataPort = 0; // 重置数据端口
                }
            });
        };

        // 如果数据连接已经就绪，立即执行
        if (state.dataSocketFuture != null && state.dataSocketFuture.isComplete() && state.dataSocketFuture.succeeded()) {
            log.info("Data channel already established, executing pending STOR command immediately.");
            state.pendingDataCommand.run();
        } else {
            log.info("Data channel not yet established, STOR command pending.");
        }
    }

     @Override
    public void stop(Future<Void> stopPromise) throws Exception {
        log.info("Stopping FTP Server Verticle...");
        disposables.clear();
        stopPromise.complete();
    }
}
