package com.mycompany.rocksdb.nfs4;

public class Nfs4Errors {
    public static final int NFS4_OK = 0;

    public static final int NFS4ERR_NOFILEHANDLE = 10006; // The filehandle is invalid
    public static final int NFS4ERR_BADHANDLE = 10007;    // The filehandle is not legal for this operation
    public static final int NFS4ERR_IO = 10008;           // I/O error
    public static final int NFS4ERR_NOTSUPP = 10021;      // Operation not supported
    public static final int NFS4ERR_SERVERFAULT = 10001;  // Unspecified server error
    public static final int NFS4ERR_ACCESS = 10003;       // Permission denied
    public static final int NFS4ERR_NOTDIR = 10024;       // Not a directory
    public static final int NFS4ERR_ISDIR = 10025;        // Is a directory
    public static final int NFS4ERR_NOENT = 10006;        // No such file or directory
    public static final int NFS4ERR_EXIST = 10017;        // File exists
    public static final int NFS4ERR_BAD_STATEID = 10022;  // Bad state ID
    public static final int NFS4ERR_INVAL = 10022;        // Invalid argument or operation
}
