package com.mycompany.rocksdb.netserver;

public class RpcConstants {
    public static final int RPC_VERSION = 2;
    public static final int NFS4_PROGRAM = 100003;
    public static final int NFS_V4 = 4;

    // RPC Message Type
    public static final int CALL = 0;
    public static final int REPLY = 1;

    // RPC Call Body
    public static final int RPC_CALL_VERSION = 2;
    public static final int AUTH_NONE = 0;
    public static final int AUTH_SYS = 1;

    // RPC Reply State
    public static final int MSG_ACCEPTED = 0;
    public static final int MSG_DENIED = 1;

    // Accepted Reply State
    public static final int SUCCESS = 0;
    public static final int PROG_UNAVAIL = 1;
    public static final int PROG_MISMATCH = 2;
    public static final int PROC_UNAVAIL = 3;
    public static final int GARBAGE_ARGS = 4;
    public static final int SYSTEM_ERR = 5;

    // NFSv4 Procedure Numbers
    public static final int NFSPROC4_NULL = 0;
    public static final int NFSPROC4_COMPOUND = 1;
    public static final int NFSPROC4_EXCHANGE_ID = 3;
    public static final int NFS4_OPEN = 10; // Placeholder, need to verify actual NFSv4 OPEN procedure number

    // Placeholder for NFSv4 State ID fields
    public static final long NFS4_STATEID_SEQID = 0x0000000000000001L;
    public static final long NFS4_STATEID_UUID = 0x0000000000000000L; // Placeholder, will be replaced by actual UUID parts

    // NFSv4 File Types
    public static final int NF4REG = 1;  // Regular file
    public static final int NF4DIR = 2;  // Directory
}
