package com.mycompany.rocksdb.smb2;

import io.vertx.reactivex.core.net.NetSocket;
import lombok.Data;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

@Data
public class Smb2ConnectionState {
    private final UUID connectionId; // Unique ID for this TCP connection
    private long sessionId; // SMB2 session ID
    private long treeConnectId; // SMB2 tree connect ID
    private long channelId;
    // private long clientGuidHigh; // Client GUID from NEGOTIATE
    // private long clientGuidLow;
    private byte[] clientGuid; // Client GUID from NEGOTIATE
    //
    private int capabilities;
    //
    private byte SecurityMode;
    // 
    private long ServerGuidLow;
    //
    private long ServerGuidHigh;
    //
    private short dialectRevision;
    //
    private Smb2FileHandle currentFileHandle;

    // Map to store open file handles
    private final Map<Long, Smb2FileHandle> openFileHandles = new ConcurrentHashMap<>();
    private final AtomicLong nextFileHandleId = new AtomicLong(1);

    public Smb2ConnectionState() {
        this.connectionId = UUID.randomUUID();
        this.sessionId = 0;
        this.treeConnectId = 0;
        this.channelId = 0;
        this.capabilities = 0;
    }

    public UUID getConnectionId() {
        return connectionId;
    }

    public long getSessionId() {
        return sessionId;
    }

    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }

    public long getTreeConnectId() {
        return treeConnectId;
    }

    public void setTreeConnectId(long treeConnectId) {
        this.treeConnectId = treeConnectId;
    }

    public long getChannelId() {
        return channelId;
    }

    public void setChannelId(long channelId) {
        this.channelId = channelId;
    }

    public byte[] getClientGuid() {
        return clientGuid;
    }

    public void setClientGuid(byte[] clientGuid) {
        this.clientGuid = clientGuid;
    }

    public Smb2FileHandle putFileHandle(long fileId, String path) {
        Smb2FileHandle handle = new Smb2FileHandle(fileId, path);
        openFileHandles.put(fileId, handle);
        return handle;
    }

    public Smb2FileHandle createNewFileHandle(String path) {
        long fileId = path.hashCode();
        Smb2FileHandle handle = new Smb2FileHandle(fileId, path);
        openFileHandles.put(fileId, handle);
        return handle;
    }

    public Smb2FileHandle getFileHandle(long fileId) {
        return openFileHandles.get(fileId);
    }

    public void removeFileHandle(long fileId) {
        openFileHandles.remove(fileId);
    }

    // Placeholder for Smb2FileHandle (will be a simple POJO)
    @Data
    public static class Smb2FileHandle {
        private static final AtomicLong volatileIdGenerator = new AtomicLong(1);
        private final long fileIdVolatile = volatileIdGenerator.getAndIncrement();
        private final long fileId;
        private final String path;
        private long lastIndex; // To track if directory has been enumerated
        // TODO: Add more file handle specific state if needed (e.g., access rights)

        public Smb2FileHandle(long fileId, String path) {
            this.fileId = fileId;
            this.path = path;
            this.lastIndex = 0;
        }
    }
}

