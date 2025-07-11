package com.mycompany.rocksdb.POJO;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.mycompany.rocksdb.POJO.Inode;
import com.mycompany.rocksdb.utils.MetaKeyUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

import java.util.*;

import static com.mycompany.rocksdb.constant.GlobalConstant.ROCKS_CHUNK_FILE_KEY;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ChunkFile {
    public long nodeId;                    // 节点ID
    public String bucket;                  // 桶名
    public String objName;                 // 对象名（文件名）
    public String versionId;               // 版本ID
    public String versionNum;              // 版本号
    public LinkedList<String> hasDeleteFiles = new LinkedList<>(); // 已删除文件列表
    public long size;                      // 总大小

    List<Inode.InodeData> chunkList = new LinkedList<>();    // 分块元数据列表

    /**
     * 通过 chunk 文件名（chunkFileName），解析出 nodeId、bucket、chunkKey 等关键信息。
     * 这为“通过 InodeData.fileName 反查归属”提供了直接的工具。
     * 譬如，InodeData = {"size":1024,"fileName":")586573301000002_/15014_dbucket_2886632c8fbc214e1a38fa042389a83f9d651_R0zk45wK2xZ9w81diPubHmka4beH6zmm","storage":"dataa","etag":"a9e754ee74fb7bb25f40555ca2c70973","chunkNum":1}
     * "fileName":")586573301000002_/15014_dbucket_2886632c8fbc214e1a38fa042389a83f9d651_R0zk45wK2xZ9w81diPubHmka4beH6zmm"
     * @param chunkFileName INodeData 中的 fileName 字段
     * @return  返回 nodeId、bucket、chunkKey(用于查找ChunkFile)
     */
    public static Tuple3<Long, String, String> getChunkFromFileName(String chunkFileName) {
        // 去掉前缀（通常是 ROCKS_CHUNK_FILE_KEY，值为 ")"），然后按下划线分割
        String[] split = chunkFileName.substring(ROCKS_CHUNK_FILE_KEY.length()).split("_");
        // 解析 nodeId（第一个字段）
        long nodeId = Long.parseLong(split[0]);
        // 还原出完整的 fileName（去掉 nodeId 和下划线）
        String fileName = chunkFileName.substring(split[0].length() + 1 + ROCKS_CHUNK_FILE_KEY.length());
        // 解析 bucket（fileName 的第二个字段）
        String bucket = fileName.split("_")[1];
        // 获取 bucket 的 vnodeId
        //String v = MetaKeyUtils.getTargetVnodeId(bucket);
        // 生成 chunkKey（通常用于元数据存储的 key）
        String chunkKey = getChunkKey("", fileName);
        // 返回 nodeId、bucket、chunkKey
        return Tuples.of(nodeId, bucket, chunkKey);
    }

    public static String getChunkKeyFromChunkFileName(String bucket, String chunkFileName) {
        String fileName = chunkFileName.substring(chunkFileName.split("_")[0].length() + 1);
        String v = MetaKeyUtils.getTargetVnodeId(bucket);
        return getChunkKey(v, fileName);
    }

    //兼容一般的key格式，增加vnode 方便迁移
    public static String getChunkKey(String vnode, String fileName) {
        return ROCKS_CHUNK_FILE_KEY + vnode + '_' + fileName;
    }

    //Inode中记录的fileName
    public static String getChunkFileName(long nodeId, String fileName) {
        return ROCKS_CHUNK_FILE_KEY + nodeId + '_' + fileName;
    }

    public static String getChunkKey(String fileName) {
        String bucket = fileName.split("_")[1];
        String v = MetaKeyUtils.getTargetVnodeId(bucket);
        return getChunkKey(v, fileName);
    }

    public static Inode.InodeData newChunk(String fileName, Inode.InodeData singleFile, Inode inode) {
        return Inode.InodeData.builder()
                .size(singleFile.size)
                .offset(0L)
                .chunkNum(1)
                .storage(inode.getStorage())
                .etag(singleFile.etag)
                .fileName(ChunkFile.getChunkFileName(inode.getNodeId(), fileName)).build();
    }

}
