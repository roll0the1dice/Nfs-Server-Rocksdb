package com.mycompany.rocksdb.nfs4;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;
import org.apache.commons.codec.binary.Hex;

/**
 * Represents the NFSv4 stateid4 structure.
 * stateid4 is composed of a sequence ID (seqid) and a 12-byte opaque field (other).
 * It uniquely identifies a client's state (e.g., an open file, a lock).
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Nfs4StateId {
    public static final int NFS4_STATEID_OTHER_BYTES = 12;
    private int seqid;
    private byte[] other; // 12 bytes

    public static Nfs4StateId fromXdr(ByteBuffer buffer) {
        int seqid = buffer.getInt();
        byte[] other = new byte[12];
        buffer.get(other);
        return new Nfs4StateId(seqid, other);
    }

    public void toXdr(ByteBuffer buffer) {
        buffer.putInt(seqid);
        buffer.put(other);
    }

    public static Nfs4StateId generateNew(int seqid) {
        // For simplicity, generate a random 12-byte 'other' field.
        // In a real implementation, this would be more carefully managed
        // to ensure uniqueness and robustness (e.g., based on server boot time, client ID, filehandle, etc.).
        byte[] other = new byte[12];
        new Random().nextBytes(other);
        return new Nfs4StateId(seqid, other);
    }

    // Equality check for stateid4
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Nfs4StateId that = (Nfs4StateId) o;
        return seqid == that.seqid && Arrays.equals(other, that.other);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(seqid);
        result = 31 * result + Arrays.hashCode(other);
        return result;
    }

    @Override
    public String toString() {
        return "Nfs4StateId{" +
               "seqid=" + seqid +
               ", other=" + Hex.encodeHexString(other) +
               '}';
    }
}
