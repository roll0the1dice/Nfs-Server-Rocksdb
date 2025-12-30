package com.mycompany.rocksdb.smb2;

import java.util.HashMap;
import java.util.Map;

public enum Smb2Command {
    SMB2_NEGOTIATE(Smb2Constants.SMB2_NEGOTIATE),
    SMB2_SESSION_SETUP(Smb2Constants.SMB2_SESSION_SETUP),
    SMB2_LOGOFF(Smb2Constants.SMB2_LOGOFF),
    SMB2_TREE_CONNECT(Smb2Constants.SMB2_TREE_CONNECT),
    SMB2_TREE_DISCONNECT(Smb2Constants.SMB2_TREE_DISCONNECT),
    SMB2_CREATE(Smb2Constants.SMB2_CREATE),
    SMB2_CLOSE(Smb2Constants.SMB2_CLOSE),
    SMB2_READ(Smb2Constants.SMB2_READ),
    SMB2_WRITE(Smb2Constants.SMB2_WRITE),
    SMB2_IOCTL(Smb2Constants.SMB2_IOCTL),
    SMB2_GET_INFO(Smb2Constants.SMB2_GET_INFO),
    SMB2_QUERY_DIRECTORY(Smb2Constants.SMB2_QUERY_DIRECTORY),
    SMB2_ECHO(0x000D), // 添加这一行
    SMB2_FLUSH(0x0007);

    private final int value;
    private static final Map<Integer, Smb2Command> BY_VALUE = new HashMap<>();

    static {
        for (Smb2Command command : values()) {
            BY_VALUE.put(command.value, command);
        }
    }

    Smb2Command(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static Smb2Command fromValue(int value) {
        return BY_VALUE.get(value);
    }
}

