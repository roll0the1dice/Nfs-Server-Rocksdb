package com.mycompany.rocksdb.nfs4;

/**
 * Defines standard UNIX file permission bits for NFSv4.
 * These correspond to the traditional S_IRWXUGO constants.
 */
public class Nfs4UnixMode {
    // User permissions
    public static final int S_IRUSR = 0000400; // Read by owner
    public static final int S_IWUSR = 0000200; // Write by owner
    public static final int S_IXUSR = 0000100; // Execute by owner

    // Group permissions
    public static final int S_IRGRP = 0000040; // Read by group
    public static final int S_IWGRP = 0000020; // Write by group
    public static final int S_IXGRP = 0000010; // Execute by group

    // Other permissions
    public static final int S_IROTH = 0000004; // Read by others
    public static final int S_IWOTH = 0000002; // Write by others
    public static final int S_IXOTH = 0000001; // Execute by others

    // Combined permissions (examples)
    public static final int S_IRWXU = (S_IRUSR | S_IWUSR | S_IXUSR); // Read, write, execute by owner
    public static final int S_IRWXG = (S_IRGRP | S_IWGRP | S_IXGRP); // Read, write, execute by group
    public static final int S_IRWXO = (S_IROTH | S_IWOTH | S_IXOTH); // Read, write, execute by others
}
