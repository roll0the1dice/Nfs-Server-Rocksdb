package com.mycompany.rocksdb.smb2;

public class Smb2Constants {
    // SMB2 Protocol Identifier
    public static final long SMB2_PROTOCOL_ID = 0xFE534D42L; // Little-endian: 0x424D53FE
    public static final long FILETIME_EPOCH_DIFF = 116444736000000000L;

    // SMB2 Command Codes (Partial list, add more as needed)
    public static final int SMB2_NEGOTIATE = 0x0000;
    public static final int SMB2_SESSION_SETUP = 0x0001;
    public static final int SMB2_LOGOFF = 0x0002;
    public static final int SMB2_TREE_CONNECT = 0x0003;
    public static final int SMB2_TREE_DISCONNECT = 0x0004;
    public static final int SMB2_CREATE = 0x0005;
    public static final int SMB2_CLOSE = 0x0006;
    public static final int SMB2_READ = 0x0008;
    public static final int SMB2_GET_INFO = 0x0010;
    public static final int SMB2_WRITE = 0x000A;
    public static final int SMB2_IOCTL = 0x000B;
    public static final int SMB2_QUERY_DIRECTORY = 0x000E; // Query directory


    // SMB2 Header Flags
    public static final int SMB2_FLAG_RESPONSE = 0x00000001; // Response message
    public static final int SMB2_FLAG_ASYNC_COMMAND = 0x00000002; // Asynchronous command
    public static final int SMB2_FLAG_RELATED_OPERATIONS = 0x00000004;
    public static final int SMB2_FLAG_SIGNED = 0x00000008; // Message is signed
    public static final int SMB2_FLAG_DFS_OPERATIONS = 0x10000000;
    public static final int SMB2_FLAG_REPLAY_OPERATION = 0x20000000;

    // SMB2 Error Codes (NTSTATUS values, partial list)
    public static final long STATUS_SUCCESS = 0x00000000L;
    public static final long STATUS_INVALID_PARAMETER = 0xC000000DL;
    public static final long STATUS_ACCESS_DENIED = 0xC0000022L;
    public static final long STATUS_NOT_SUPPORTED = 0xC00000BBL;
    public static final long STATUS_BAD_NETWORK_NAME = 0xC00000CC;
    public static final long STATUS_NO_MEMORY = 0xC0000017L;
    public static final long STATUS_INTERNAL_ERROR = 0xC00000E5L;
    public static final long STATUS_FILE_NOT_FOUND = 0xC00000F0L;
    public static final long STATUS_USER_SESSION_NOT_FOUND = 0xC0000203L;
    public static final long STATUS_INVALID_HANDLE = 0xC0000008L;
    public static final long STATUS_NETWORK_NAME_DELETED = 0xC00000C9L;
    public static final long STATUS_OBJECT_NAME_NOT_FOUND = 0xC0000034L;
    public static final long STATUS_OBJECT_NAME_COLLISION = 0xC0000035L ;

    // Generic Access Rights (from Windows ACCESS_MASK)
    public static final int GENERIC_READ = 0x80000000;
    public static final int GENERIC_WRITE = 0x40000000;
    public static final int GENERIC_EXECUTE = 0x20000000;
    public static final int FILE_GENERIC_READ = 0x00120089;
    public static final int FILE_GENERIC_WRITE = 0x00120116;
    public static final int FILE_GENERIC_EXECUTE = 0x001200A0;

    // Share Access Rights (from Windows FILE_SHARE_XXX)
    public static final int FILE_SHARE_READ = 0x00000001;
    public static final int FILE_SHARE_WRITE = 0x00000002;
    public static final int FILE_SHARE_DELETE = 0x00000004;

    // Create Disposition (from Windows FILE_DISPOSITION_XXX)
    public static final int FILE_SUPERSEDE = 0x00000000;
    public static final int FILE_OPEN = 0x00000001;
    public static final int FILE_CREATE = 0x00000002;
    public static final int FILE_OPEN_IF = 0x00000003;
    public static final int FILE_OVERWRITE = 0x00000004;
    public static final int FILE_OVERWRITE_IF = 0x00000005;

    // Create Options (from Windows FILE_ATTRIBUTE_XXX and FILE_FLAG_XXX)
    public static final int FILE_DIRECTORY_FILE = 0x00000001; // Create a directory
    public static final int FILE_WRITE_THROUGH = 0x00000002;
    public static final int FILE_SEQUENTIAL_ONLY = 0x00000004;
    public static final int FILE_NO_INTERMEDIATE_BUFFERING = 0x00000008;
    public static final int FILE_NON_DIRECTORY_FILE = 0x00000040; // Create a non-directory file
    public static final int FILE_SYNCHRONOUS_IO_ALERT = 0x00000010;
    public static final int FILE_SYNCHRONOUS_IO_NONALERT = 0x00000020;
    public static final int FILE_DELETE_ON_CLOSE = 0x00001000;

    // Max buffer sizes
    public static final int SMB2_MAX_BUFFER_SIZE = 1024 * 1024; // Example: 1MB

    // Negotiate Dialects
    public static final short SMB2_DIALECT_002 = 0x0202; // SMB 2.0.2
    public static final short SMB2_DIALECT_021 = 0x0210; // SMB 2.1
    public static final short SMB3_DIALECT_030 = 0x0300; // SMB 3.0
    public static final short SMB3_DIALECT_0302 = 0x0302; // SMB 3.0.2
    public static final short SMB3_DIALECT_0311 = 0x0311; // SMB 3.1.1
    /**
     * SMB2 Header Flags: SMB2_FLAGS_SERVER_TO_REDIR (0x00000001)
     * 当此位被设置时，表示该消息是从服务器发往客户端的响应 (Response)。
     * 如果此位为 0，则表示该消息是客户端发往服务器的请求 (Request)。
     */
    public static final int SMB2_FLAGS_SERVER_TO_REDIR = 0x00000001;

    /**
     * SMB2 Command: SMB2_COMMAND_NEGOTIATE (0x0000)
     * 协商协议命令。这是连接建立后的第一个交互命令。
     */
    public static final short SMB2_COMMAND_NEGOTIATE = 0x0000;
    
    // 顺便建议你定义其他后续会用到的命令：
    public static final short SMB2_COMMAND_SESSION_SETUP    = 0x0001; // 会话设置/登录
    public static final short SMB2_COMMAND_LOGOFF           = 0x0002; // 注销
    public static final short SMB2_COMMAND_TREE_CONNECT     = 0x0003; // 连接共享
    public static final short SMB2_COMMAND_TREE_DISCONNECT  = 0x0004; // 断开共享
    public static final short SMB2_COMMAND_CREATE           = 0x0005; // 打开/创建文件
    public static final short SMB2_COMMAND_CLOSE            = 0x0006; // 关闭文件
    public static final short SMB2_COMMAND_READ             = 0x0008; // 读取文件
    public static final short SMB2_COMMAND_QUERY_INFO       = 0x0009; // 查询信息
    public static final short SMB2_COMMAND_QUERY_DIRECTORY  = 0x000A; // 查询目录
    public static final short SMB2_COMMAND_WRITE            = 0x000B; // 写入文件


        // File Information Classes (for QUERY_INFO and QUERY_DIRECTORY)
    public static final byte FILE_DIRECTORY_INFORMATION = 0x01; // For QueryDirectory
    public static final byte FILE_FULL_DIRECTORY_INFORMATION = 0x02; // For QueryDirectory
    public static final byte FILE_ID_BOTH_DIRECTORY_INFORMATION = 0x26; // For QueryDirectory
    public static final byte FILE_NAMES_INFORMATION = 0x0C; // For QueryDirectory
    public static final byte FILE_BASIC_INFORMATION = 0x04; // For QueryInfo
    public static final byte FILE_STANDARD_INFORMATION = 0x05; // For QueryInfo
    public static final byte FILE_INTERNAL_INFORMATION = 0x06; // For QueryInfo
    public static final byte FILE_ALL_INFO = 0x12; // For QueryInfo

    // Capabilities
    public static final int SMB2_GLOBAL_CAP_DFS = 0x00000001;
    public static final int SMB2_GLOBAL_CAP_LEASING = 0x00000002;
    public static final int SMB2_GLOBAL_CAP_LARGE_MTU = 0x00000004;
    public static final int SMB2_GLOBAL_CAP_MULTI_CHANNEL = 0x00000008;
    public static final int SMB2_GLOBAL_CAP_PERSISTENT_HANDLES = 0x00000010;
    public static final int SMB2_GLOBAL_CAP_DIRECTORY_LEASING = 0x00000020;

    // Security Mode
    public static final short SMB2_NEGOTIATE_SIGNING_ENABLED = 0x0001;
    public static final short SMB2_NEGOTIATE_SIGNING_REQUIRED = 0x0002;

    // Context types for Negotiate context
    public static final short SMB2_NEGOTIATE_CONTEXT_NETNAME_NEGOTIATE_CONTEXT_ID = 0x0001; // Not strictly a type, but an ID

    // Session Setup Flags
    public static final int SMB2_SESSION_FLAG_BINDING = 0x0001;
    public static final int SMB2_SESSION_FLAG_IS_GUEST = 0x0002;
    public static final int SMB2_SESSION_FLAG_IS_NULL = 0x0004;
    public static final int SMB2_SESSION_FLAG_ENCRYPT_DATA = 0x0008;

    // Tree Connect Flags
    public static final int SMB2_TREE_CONNECT_FLAG_CLUSTER_RECONNECT = 0x0001;
    public static final int SMB2_TREE_CONNECT_FLAG_DFS_PATHNAME = 0x0002;
    public static final int SMB2_TREE_CONNECT_FLAG_EXTENDED_RESPONSES = 0x0004;


    // NTSTATUS
    public static final long STATUS_BUFFER_OVERFLOW = 0x80000005L;
    public static final long STATUS_FILE_CLOSED = 0xC0000128L;

    // File Attributes
    public static final int FILE_ATTRIBUTE_READONLY = 0x00000001;
    public static final int FILE_ATTRIBUTE_HIDDEN = 0x00000002;
    public static final int FILE_ATTRIBUTE_DIRECTORY = 0x00000010;

    // Close Flags
    public static final int SMB2_CLOSE_FLAG_POSTQUERY_ATTRIB = 0x0001;

    public static final int SMB2_0_INFO_FILE = 0x01;
    public static final int SMB2_0_INFO_FILESYSTEM = 0x02;
    public static final int FILE_FS_DEVICE_INFORMATION = 0x04;
    public static final int FILE_FS_ATTRIBUTE_INFORMATION = 0x05;
    public static final int FILE_FS_FULL_SIZE_INFORMATION = 0x07;
    public static final int FILE_FS_VOLUME_INFORMATION = 0x01;
    public static final int FILE_FS_SIZE_INFORMATION = 0x03;
    public static final int FILE_FS_SECTION_INFORMATION = 0x0B;

    public static final long STATUS_NOT_A_DIRECTORY = 0xC0000103L; // Not a directory
    // 客户端提供的缓冲区（OutputBuffer）太小，甚至无法装下请求的信息类（Information Class）的一个完整结构。
    public static final long STATUS_INFO_LENGTH_MISMATCH = 0xC0000004L;
    public static final long STATUS_END_OF_FILE = 0xC0000011;

    public static final long STATUS_NO_SUCH_FILE = 0xC000000FL; // No such file
    public static final long STATUS_NO_MORE_FILES = 0x80000006L; // No more files

}

