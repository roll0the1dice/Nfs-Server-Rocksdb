package com.mycompany.rocksdb.POJO;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.rocksdb.utils.VersionUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.mycompany.rocksdb.constant.GlobalConstant.ROCKS_INODE_PREFIX;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Inode {
    @JsonProperty("mode")
    int mode;
    @JsonProperty("cifsMode")
    int cifsMode;
    @JsonProperty("size")
    long size;
    @JsonProperty("nodeId")
    long nodeId;
    @JsonProperty("atime")
    long atime;
    @JsonProperty("mtime")
    long mtime;
    @JsonProperty("ctime")
    long ctime;
    @JsonProperty("createTime")
    long createTime;
    @JsonProperty("atimensec")
    int atimensec;
    @JsonProperty("mtimensec")
    int mtimensec;
    @JsonProperty("ctimensec")
    int ctimensec;
    @JsonProperty("linkN")
    int linkN;
    @JsonProperty("uid")
    int uid;
    @JsonProperty("gid")
    int gid;
    @JsonProperty("majorDev")
    int majorDev;
    @JsonProperty("minorDev")
    int minorDev;
    @JsonProperty("bucket")
    String bucket;
    @JsonProperty("objName")
    String objName;
    @JsonProperty("versionId")
    String versionId;
    //软连接
    @JsonProperty("reference")
    String reference;
    @JsonProperty("versionNum")
    String versionNum;
    // 在create时记录存储池，确保多存储池上传情况中所有数据块在同一存储池中
    @JsonProperty("storage")
    String storage;
    @JsonProperty("cookie")
    long cookie;
    @JsonProperty("inodeData")
    List<InodeData> inodeData;

    List<ChunkFile> updatedChunkFile;

    public static Inode RETRY_INODE = Inode.builder().linkN(-3).build();

    public Inode clone() {
        return  Inode.builder()
                .mode(mode)
                .size(size)
                .nodeId(nodeId)
                .bucket(bucket)
                .objName(objName)
                .versionNum(versionNum)
                .versionId(versionId)
                .linkN(linkN)
                .reference(reference)
                .cookie(cookie)
                .gid(gid)
                .uid(uid)
                .cifsMode(cifsMode)
                .majorDev(majorDev)
                .minorDev(minorDev)
                .build();
    }

    public static String getKey(String vnode, String bucket, long nodeId) {
        return ROCKS_INODE_PREFIX + vnode + File.separator + bucket + File.separator
                + nodeId;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @Builder
    public static class InodeData {
        @JsonIgnore
        public long offset;
        public long size;
        public String fileName;
        public String storage;
        public String etag;
        public int chunkNum;
        public InodeData(InodeData inodeData) {
            this.offset = inodeData.offset;
            this.size = inodeData.size;
            this.fileName = inodeData.fileName;
            this.storage = inodeData.storage;
            this.etag = inodeData.etag;
            this.chunkNum = inodeData.chunkNum;
        }


        public static InodeData newHoleFile(long size) {
            return  InodeData.builder()
                    .fileName("")
                    .offset(0L)
                    .size(size)
                    .etag("")
                    .storage("")
                    .build();
        }

        public String fetchInodeDataTargetVnodeId() {
            if (StringUtils.isNotBlank(fileName)) {
                return fileName.split("_")[0].substring(1);
            }
            return "";
        }
    }

    public static Inode defaultInode(VersionIndexMetadata metaData) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        List<InodeData> data = new LinkedList<>();
        Map<String, String> sysMetaMap = objectMapper.readValue(metaData.getSysMetaData().getBytes(StandardCharsets.UTF_8), new TypeReference<Map<String, String>>() {
        });
        if (StringUtils.isNotBlank(metaData.getFileName())) {
            data.add(InodeData.builder()
                    .size(metaData.getEndIndex() + 1)
                    .storage(metaData.getStorage())
                    .fileName(metaData.getFileName())
                    .etag(sysMetaMap.get("ETag"))
                    .offset(0L).build());
        }

        return  Inode.builder()
                .mode(33188)
                .cifsMode(32)
                .size(metaData.getEndIndex() + 1)
                .objName(metaData.getKey())
                .bucket(metaData.getBucket())
                .reference(metaData.getKey())
                .storage(metaData.getStorage())
                .linkN(1)
                .versionNum(metaData.getVersionNum())
                .versionId("null")
                .nodeId(VersionUtil.newInode())
                .inodeData(data)
                .uid(0)
                .gid(0)
                .build();
    }



//    public List<InodeData> cloneInodeData(List<InodeData> inodeData) {
//        List<InodeData> newInodeData = new LinkedList<>();
//        for (InodeData data : inodeData) {
//            InodeData inodeData0 = new InodeData();
//            inodeData0.setFileName(data.getFileName());
//            inodeData0.setSize(data.getSize());
//            inodeData0.setStorage(data.getStorage());
//            inodeData0.setOffset(data.getOffset());
//            inodeData0.setEtag(data.getEtag());
//            inodeData0.setChunkNum(data.getChunkNum());
//            newInodeData.add(inodeData0);
//        }
//        return newInodeData;
//    }
    public static void partialOverwrite(ChunkFile chunkFile, long coverOffset, Inode.InodeData updatedData) {
    ListIterator<Inode.InodeData> it = chunkFile.chunkList.listIterator();
    List<Inode.InodeData> newChunks = new ArrayList<>();

    List<InodeData> deleteList = new LinkedList<>();
    boolean dataAdded = false;

    long coverEnd = coverOffset + updatedData.size;
    long curOffset = 0L;
        while (it.hasNext()) {
            InodeData cur = it.next();
            long curEnd = curOffset + cur.size;

            // 1. 当前块在覆盖区间左侧，无重叠
            //                     [coverOffset, coverEnd]
            // [curOffset, curEnd]
            if (curEnd < coverOffset) {
                curOffset = curEnd;
                continue;
            }

            // 2. 当前块在覆盖区间右侧，无重叠
            // [coverOffset, coverEnd]
            //                          [curOffset, curEnd]
            if (coverEnd < curOffset) {
                break;
            }

            //        [coverOffset, coverEnd]
            //  [curOffset, curEnd]
            // 3.3 左侧部分重叠：curOffset < coverOffset < curEnd <= coverEnd
            if (curOffset < coverOffset && curEnd <= coverEnd) {
                // 保留前半部分
                cur.size = coverOffset - curOffset;

                // 覆盖段
                if (!dataAdded) {
                    it.add(updatedData);
                    dataAdded = true;
                }

                curOffset = curEnd;
                continue;
            }

            //  [coverOffset, coverEnd]
            //          [curOffset, curEnd]
            // 3.4 右侧部分重叠：coverOffset <= curOffset < coverEnd < curEnd
            if (coverOffset <= curOffset && coverEnd < curEnd) {
                // 覆盖段
                if (!dataAdded) {
                    it.add(updatedData);
                    dataAdded = true;
                }

                // 保留后半部分
                cur.offset = cur.offset + (coverEnd - curOffset);
                cur.size = curEnd - coverEnd;

                curOffset = curEnd;
                continue;
            }

            // 走到这里，必然有重叠：curEnd > coverOffset && coverEnd > curOffset
            // 3. 有重叠
            // [coverOffset, coverEnd]
            //   [curOffset, curEnd]
            // 3.1 完全覆盖：coverOffset <= curOffset && curEnd <= coverEnd
            if (coverOffset <= curOffset && curEnd <= coverEnd) {
                deleteList.add(cur);
                it.remove();

                if (!dataAdded) {
                    it.add(updatedData);
                    dataAdded = true;
                }

                curOffset = curEnd;
                continue;
            }

            //    [coverOffset, coverEnd]
            //  [curOffset    ,     curEnd]
            // 3.2 被包裹：curOffset < coverOffset && coverEnd < curEnd
            if (curOffset < coverOffset && coverEnd < curEnd) {
                // 拆成三段：前段、覆盖段、后段
                // 前段
                cur.size = coverOffset - curOffset;
                // 覆盖段
                if (!dataAdded) {
                    it.add(updatedData);
                    dataAdded = true;
                }
                // 后段
                InodeData right = new InodeData(cur);
                right.offset = cur.offset + (coverEnd - curOffset);
                right.size = curEnd - coverEnd;

                it.add(right);

                curOffset = curEnd;
                continue;
            }
        }

        if (!dataAdded) {
            if (curOffset < coverOffset) {
                long newSize = coverOffset - curOffset;
                chunkFile.chunkList.add(new InodeData(curOffset, newSize, "", "", "", 0));
            }
            chunkFile.chunkList.add(updatedData);
        }

    }

    public static long partialOverwrite3(ChunkFile chunkFile, long coverOffset, Inode.InodeData updatedData) {
        long totalOffset = 0;
        ListIterator<Inode.InodeData> it = chunkFile.chunkList.listIterator();
        boolean dataAdded = false;

        long curOffset = 0L;

        long totalOverwrittenSize = 0;

        while (it.hasNext()) {
            Inode.InodeData cur = it.next();
            cur.offset = totalOffset;
            totalOffset += cur.size;

            long curEnd = curOffset + cur.size;
            long coverEnd = coverOffset + updatedData.size;

            long overlapStart = Math.max(curOffset, coverOffset);
            long overlapEnd = Math.min(curEnd, coverEnd);

            if (overlapEnd > overlapStart) {
                totalOverwrittenSize += (overlapEnd - overlapStart);
            }

            // Case 1: Current chunk is completely to the left of the overwrite area.
            if (curEnd <= coverOffset) {
                curOffset = curEnd;
                continue;
            }

            // Case 2: Current chunk is completely to the right of the overwrite area.

            if (curOffset >= coverEnd) {
                // The overwrite area is in a gap before this chunk.
                // Go back and insert.
                if (!dataAdded) {
                    it.previous();
                    it.add(updatedData);
                    dataAdded = true;
                }
                break;
            }

            // From here, we know there is some overlap.

            // Case 3.2: Current chunk envelops the new data. Split into three.
            if (curOffset < coverOffset && curEnd > coverEnd) {
                // Part 1: Left part (modify current chunk)
                cur.size = coverOffset - curOffset;

                if (!dataAdded) {
                    // Part 2: Middle part (the new data)
                    it.add(updatedData);
                    dataAdded = true;
                }

                // Part 3: Right part (create a new chunk)
                Inode.InodeData right = new Inode.InodeData(cur);
                right.offset = cur.offset + (coverEnd - curOffset);
                right.size = curEnd - coverEnd;
                it.add(right);

                //dataAdded = true;
                // Since we've fully handled the overwrite, we can stop.
                break;
            }

            // Case 3.3: Left overlap. New data overwrites the tail of the current chunk.
            if (curOffset < coverOffset && curEnd <= coverEnd) {
                // Truncate the current chunk
                cur.size = coverOffset - curOffset;
                // The new data will be handled by subsequent logic.
                // We don't add it here because it might span multiple chunks.
            }

            // Case 3.4: Right overlap. New data overwrites the head of the current chunk.
            if (curOffset >= coverOffset && curEnd > coverEnd) {
                if (!dataAdded) {
                    // Go back to insert before the current chunk
                    it.previous();
                    it.add(updatedData);
                    dataAdded = true;
                    // Move iterator forward past the newly added element
                    it.next();
                }
                // Modify the current chunk to represent the remaining part.
                cur.offset = cur.offset + (coverEnd - curOffset);
                cur.size = curEnd - coverEnd;
            }

            // Case 3.1: Complete overwrite. Current chunk is fully within the new data's range.
            if (curOffset >= coverOffset && curEnd <= coverEnd) {
                if (!dataAdded) {
                    // Go back, add the new data, then remove the current chunk.
                    it.previous();
                    it.add(updatedData);
                    dataAdded = true;
                    // Move iterator forward to get back to 'cur'
                    it.next();
                }
                if(!chunkFile.hasDeleteFiles.contains(cur.getFileName())) {
                    chunkFile.hasDeleteFiles.add(cur.getFileName());
                }
                it.remove();
            }

            curOffset = curEnd;
        }

        return totalOverwrittenSize;
    }

    public static void partialOverwrite2(ChunkFile chunkFile, long coverOffset, Inode.InodeData updatedData) {
        ListIterator<Inode.InodeData> it = chunkFile.chunkList.listIterator();
        List<Inode.InodeData> newChunks = new ArrayList<>();

        List<InodeData> deleteList = new LinkedList<>();
        boolean addData = false;

        long coverEnd = coverOffset + updatedData.size;
        long curOffset = 0L;
        while (it.hasNext()) {
            Inode.InodeData cur = it.next();
            // 当前InodeData块的区间[curOffset, curEnd]
            long curEnd = curOffset + cur.size;

            // 无重叠,完全右侧
            //                     [coverOffset, coverEnd]
            // [curOffset, curEnd]
            if (curEnd < coverOffset) {
                curOffset = curEnd;
            }
            // 完全左侧
            // [coverOffset, coverEnd]
            //                          [curOffset, curEnd]
            else if (coverEnd < curOffset) {
                break;
            }
            // 有重叠
            // 必然有 curEnd >= coverOffset && coverEnd >= curOffset 成立
            else {
               if (addData) {
                   // 只覆盖前半部分
                   // 保留后半部分，调整 offset/size
                   if (coverEnd < curEnd) {
                       // [coverOffset, coverEnd]
                       //                [curOffset, curEnd]
                       long newSize = curEnd - coverEnd;
                       cur.offset += cur.size - newSize;
                       cur.size = newSize;
                   }
                   //  [coverOffset, coverEnd]
                   //  [curOffset,     curEnd]
                   // coverEnd >= curEnd && coverEnd >= curOffset
                   // curEnd >= coverOffset
                   // 区间重合
                   else {
                       deleteList.add(cur);
                       it.remove();
                   }
               } else {
                   // 覆盖
                   // [coverOffset, coverEnd]
                   // [curOffset, curEnd]
                   if (coverOffset == curOffset && coverEnd >= curEnd) {
                       deleteList.add(cur);
                       it.remove();
                   } else {
                       cur.size = coverOffset - curOffset;
                   }

                   it.add(updatedData);
                   addData = true;
                   //           [curOffset, curEnd]
                   // [coverOffset, coverEnd]
                   if (coverEnd < curEnd) {
                        long newSize = curEnd - coverEnd;
                        long offset = cur.offset + cur.size - newSize;
                        InodeData next = new InodeData(newSize, offset, cur.fileName, "", "", 0);

                        it.add(next);
                   }
               }

               curOffset = curEnd;
            }
        }
        // 合并相邻同类型块（可选，简化演示不做）
        //chunkFile.chunkList = newChunks;
        if (!addData) {
            if (curOffset < coverOffset) {
                long newSize = coverOffset - curOffset;
                chunkFile.chunkList.add(new InodeData(0, newSize, "", "", "", 0));
            }
            updatedData.offset = 0;
            chunkFile.chunkList.add(updatedData);
        }
    }


}
