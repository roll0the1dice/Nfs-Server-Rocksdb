package com.mycompany.rocksdb.nfs4;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 定义了 NFSv4 协议中的所有操作码 (NFS4code)。
 * 这些值基于 RFC 7530 (NFSv4.0), RFC 5661 (NFSv4.1), 和 RFC 8881 (NFSv4.2)。
 */
public enum Nfs4Opcode  {

    //================================================================
    // NFSv4.0 NFS4erations (RFC 7530)
    //================================================================
    NFS4_ACCESS(3),
    NFS4_CLOSE(4),
    NFS4_COMMIT(5),
    NFS4_CREATE(6),
    NFS4_DELEGPURGE(7),
    NFS4_DELEGRETURN(8),
    NFS4_GETATTR(9),
    NFS4_GETFH(10),
    NFS4_LINK(11),
    NFS4_LOCK(12),
    NFS4_LOCKT(13),
    NFS4_LOCKU(14),
    NFS4_LOOKUP(15),
    NFS4_LOOKUPP(16),
    NFS4_NVERIFY(17),
    NFS4_OPEN(18),
    NFS4_OPEN_CONFIRM(20),
    NFS4_NFS4EN_DOWNGRADE(21),
    NFS4_PUTFH(22),
    NFS4_PUTPUBFH(23),
    NFS4_PUTROOTFH(24),
    NFS4_READ(25),
    NFS4_READDIR(26),
    NFS4_READLINK(27),
    NFS4_REMOVE(28),
    NFS4_RENAME(29),
    NFS4_RENEW(30),
    NFS4_RESTOREFH(31),
    NFS4_SAVEFH(32),
    NFS4_SECINFO(33),
    NFS4_SETATTR(34),
    NFS4_SETCLIENTID(35),
    NFS4_SETCLIENTID_CONFIRM(36),
    NFS4_VERIFY(37),
    NFS4_WRITE(38),
    NFS4_RELEASE_LOCKOWNER(39),
    //NFS4_RELEASE_LOCKID(40),
    //NFS4_DELEGRETURN(41),

    //================================================================
    // NFSv4.1 NFS4erations (RFC 5661)
    //================================================================
    NFS4_BACKCHANNEL_CTL(40),
    NFS4_BIND_CONN_TO_SESSION(41),
    NFS4_EXCHANGE_ID(42),
    NFS4_CREATE_SESSION(43),
    NFS4_DESTROY_SESSION(44),
    NFS4_FREE_STATEID(45),
    NFS4_GET_DIR_DELEGATION(46),
    NFS4_GETDEVICEINFO(47),
    NFS4_GETDEVICELIST(48),
    NFS4_LAYOUTCOMMIT(49),
    NFS4_LAYOUTGET(50),
    NFS4_LAYOUTRETURN(51),
    NFS4_SECINFO_NO_NAME(52),
    NFS4_SEQUENCE(53),
    NFS4_SET_SSV(54),
    NFS4_TEST_STATEID(55),
    //NFS4_DESTROY_CLIENTID(56),
    NFS4_WANT_DELEGATION(56),
    NFS4_DESTROY_CLIENTID(57),
    NFS4_RECLAIM_COMPLETE(58),

    //================================================================
    // NFSv4.2 NFS4erations (RFC 8881)
    //================================================================
    NFS4_ALLOCATE(59),
    NFS4_CNFS4Y(60),
    NFS4_CNFS4Y_NOTIFY(61),
    NFS4_DEALLOCATE(62),
    NFS4_IO_ADVISE(63),
    NFS4_LAYOUTERROR(64),
    NFS4_LAYOUTSTATS(65),
    NFS4_OFFLOAD_CANCEL(66),
    NFS4_OFFLOAD_STATUS(67),
    NFS4_READ_PLUS(68),
    NFS4_SEEK(69),
    NFS4_WRITE_SAME(70),
    NFS4_CLONE(71),

    // Special value for unknown or unsupported NFS4erations
    NFS4_ILLEGAL(10044);

    private final int value;

    /**
     * 用于高效反向查找的静态 Map。
     * Key 是整数操作码，Value 是对应的枚举常量。
     */
    private static final Map<Integer, Nfs4Opcode> BY_VALUE =
            Collections.unmodifiableMap(Stream.of(values())
                    .collect(Collectors.toMap(Nfs4Opcode::getValue, Function.identity())));

    Nfs4Opcode(int value) {
        this.value = value;
    }

    /**
     * @return 操作码对应的整数值。
     */
    public int getValue() {
        return value;
    }

    /**
     * 根据整数值查找对应的 NfsNFS4code 枚举常量。
     *
     * @param value 从网络流中读取的操作码整数。
     * @return 对应的枚举常量。如果找不到，则返回 NFS4_ILLEGAL。
     */
    public static Nfs4Opcode fromValue(int value) {
        return BY_VALUE.getOrDefault(value, NFS4_ILLEGAL);
    }

    public static void main(String[] args) {
        // --- 使用示例 ---

        // 1. 获取操作码的整数值
        System.out.println("EXCHANGE_ID 的值是: " + Nfs4Opcode.NFS4_EXCHANGE_ID.getValue()); // 输出 42
        System.out.println("GETATTR 的值是: " + Nfs4Opcode.NFS4_GETATTR.getValue());       // 输出 9

        // 2. 从整数值反向查找枚举（模拟从网络解析）
        int receivedNFS4codeValue = 42;
        Nfs4Opcode NFS4 = Nfs4Opcode.fromValue(receivedNFS4codeValue);
        System.out.println("接收到的值 " + receivedNFS4codeValue + " 对应操作: " + NFS4); // 输出 NFS4_EXCHANGE_ID

        int unknownNFS4codeValue = 999;
        Nfs4Opcode unknownNFS4 = Nfs4Opcode.fromValue(unknownNFS4codeValue);
        System.out.println("接收到的值 " + unknownNFS4codeValue + " 对应操作: " + unknownNFS4); // 输出 NFS4_ILLEGAL
    }
}