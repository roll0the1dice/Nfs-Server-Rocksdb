package com.mycompany.rocksdb.nfs4;

/**
 * Defined NFSv4 ACCESS control bits.
 * Based on RFC 7530, Section 18.27.4 ACCESS4_MASK and ACCESS4_MODE.
 */
public class Nfs4Access {
    public static final int ACCESS4_READ    = 0x00000001; // read data / read directory
    public static final int ACCESS4_LOOKUP  = 0x00000002; // look up a name in a directory
    public static final int ACCESS4_MODIFY  = 0x00000004; // write data / create / setattr / link / symlink
    public static final int ACCESS4_EXTEND  = 0x00000008; // append data / create
    public static final int ACCESS4_DELETE  = 0x00000010; // delete a file or directory entry
    public static final int ACCESS4_EXECUTE = 0x00000020; // execute a file / search a directory
}
