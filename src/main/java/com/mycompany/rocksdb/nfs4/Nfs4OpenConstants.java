package com.mycompany.rocksdb.nfs4;

/**
 * Defines constants related to NFSv4 OPEN operation.
 * Based on RFC 7530, Section 18.23 OPEN operation.
 */
public class Nfs4OpenConstants {

    // open_flag4 (oa_type)
    public static final int OPEN4_CREATE_NONE = 0;
    public static final int OPEN4_CREATE = 1;
    public static final int OPEN4_NOCREATE = 0;

    // create_mode4 (create_how)
    public static final int OPEN4_CREATE_UNCHECKED = 0; // Check for existence but don't fail on non-existence
    public static final int OPEN4_CREATE_GUARDED = 1;   // Check for existence and fail on existence
    public static final int OPEN4_CREATE_EXCLUSIVE = 2; // Exclusive create, no attributes needed

    // claim_type4 (cl_type)
    public static final int OPEN4_CLAIM_NULL = 0;           // No claim, file identified by currentFH and name
    public static final int OPEN4_CLAIM_PREVIOUS = 1;       // Client claims it previously had a lease
    public static final int OPEN4_CLAIM_DELEGATE_CUR = 2;   // Client claims delegation on currentFH
    public static final int OPEN4_CLAIM_DELEGATE_PREV = 3;  // Client claims delegation on previousFH
    public static final int OPEN4_CLAIM_FH = 4;             // Client provides an explicit FH
    public static final int OPEN4_CLAIM_DELEG_CUR_FH = 5;   // Client claims delegation on currentFH (with FH)
    public static final int OPEN4_CLAIM_DELEG_PREV_FH = 6;  // Client claims delegation on previousFH (with FH)

    // open_res_flags4 (opr_flags)
    public static final int OPEN4_RESULT_LOCK_DENIED = 0x00000001; // Lock denied by server
    public static final int OPEN4_RESULT_CONFIRM = 0x00000002;     // Client should confirm the open
    public static final int OPEN4_RESULT_DELEG_CUR = 0x00000004;   // Delegation granted (current)
    public static final int OPEN4_RESULT_DELEG_PREV = 0x00000008;  // Delegation granted (previous)
    public static final int OPEN4_RESULT_RECLAIM = 0x00000010;     // Client should reclaim state
    public static final int OPEN4_RESULT_FILE_EXISTS = 0x00000020; // File already exists
    public static final int OPEN4_RESULT_FILE_CREATED = 0x00000040; // File was created

    // share_access4
    public static final int ACCESS4_READ = 0x00000001;
    public static final int ACCESS4_WRITE = 0x00000002;
    public static final int ACCESS4_BOTH = 0x00000003;

    // share_deny4
    public static final int DENY4_NONE = 0x00000000;
    public static final int DENY4_SHARE_READ = 0x00000001;
    public static final int DENY4_SHARE_WRITE = 0x00000002;
    public static final int DENY4_SHARE_BOTH = 0x00000003;

}
