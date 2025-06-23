package com.mycompany.rocksdb.netserver;

public enum RpcParseState {
    READING_MARKER,
    READING_FRAGMENT_DATA
}
