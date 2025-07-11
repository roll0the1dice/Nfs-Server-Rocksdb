package com.mycompany.rocksdb.POJO;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
public class VersionIndexMetadata {
  @JsonProperty("sysMetaData")
  private String sysMetaData;

  //  // 1. 生成 RFC 1123 格式的日期时间字符串
  //  ZonedDateTime dateTime = ZonedDateTime.of(2025, 4, 17, 11, 27, 8, 0, ZoneOffset.UTC);
  //  String value = dateTime.format(DateTimeFormatter.RFC_1123_DATE_TIME);
  //  // 2. 使用 Map 存储键值对
  //  Map<String, Object> dataMap = new HashMap<>();
  //  dataMap.put(key, value);
  //  // 3. 使用 Jackson 的 ObjectMapper 将 Map 转换成 JSON 字符串
  //  ObjectMapper objectMapper = new ObjectMapper();
  //  String jsonString = objectMapper.writeValueAsString(dataMap);
  @JsonProperty("userMetaData")
  private String userMetaData;

  @JsonProperty("objectAcl")
  private String objectAcl;

  @JsonProperty("fileName")
  private String fileName;

//  @JsonProperty("startIndex")
//  private long startIndex; // 使用 long 类型以防数字过大

  @JsonProperty("endIndex")
  private long endIndex; // 使用 long 类型以防数字过大

  @JsonProperty("versionNum")
  private String versionNum;

  @JsonProperty("syncStamp")
  private String syncStamp;

  @JsonProperty("shardingStamp")
  private String shardingStamp;

  @JsonProperty("stamp")
  private long stamp; // 时间戳通常是 long 类型

  @JsonProperty("storage")
  private String storage;

  @JsonProperty("key")
  private String key;

  @JsonProperty("bucket")
  private String bucket;

  @JsonProperty("inode")
  private long inode;

  @JsonProperty("cookie")
  private long cookie;

  public static LatestIndexMetadata toLatestIndexMetadata(VersionIndexMetadata indexMetadata) {
    if (indexMetadata == null) {
      return null;
    }

    LatestIndexMetadata minusIndexMetadata = new LatestIndexMetadata();

    // 复制共有的字段
    minusIndexMetadata.setSysMetaData(indexMetadata.getSysMetaData());
    minusIndexMetadata.setEndIndex(indexMetadata.getEndIndex());
    minusIndexMetadata.setVersionNum(indexMetadata.getVersionNum());
    minusIndexMetadata.setSyncStamp(indexMetadata.getSyncStamp());
    minusIndexMetadata.setStamp(indexMetadata.getStamp());
    minusIndexMetadata.setStorage(indexMetadata.getStorage());
    minusIndexMetadata.setKey(indexMetadata.getKey());
    minusIndexMetadata.setBucket(indexMetadata.getBucket());
    minusIndexMetadata.setInode(indexMetadata.getInode());
    minusIndexMetadata.setCookie(indexMetadata.getCookie());

    // 对于 MinusIndexMetadata 中有而 IndexMetadata 中没有的字段 (shardingStamp)，
    // 这里会保留其默认值 (null)。如果需要从其他地方获取，可以在这里设置。
    minusIndexMetadata.setShardingStamp(StringIncrementer.incrementNumberAfterHyphen(indexMetadata.getSyncStamp()));

    return minusIndexMetadata;
  }

  public static IndexMetadata toIndexMetadata(VersionIndexMetadata versionIndexMetadata) {
    if (versionIndexMetadata == null) {
      return null;
    }

    IndexMetadata indexMetadata = new IndexMetadata();

    // 复制共有的字段
    indexMetadata.setSysMetaData(versionIndexMetadata.getSysMetaData());
    indexMetadata.setEndIndex(versionIndexMetadata.getEndIndex());
    indexMetadata.setVersionNum(versionIndexMetadata.getVersionNum());
    indexMetadata.setSyncStamp(versionIndexMetadata.getSyncStamp());
    indexMetadata.setStamp(versionIndexMetadata.getStamp());
    indexMetadata.setStorage(versionIndexMetadata.getStorage());
    indexMetadata.setKey(versionIndexMetadata.getKey());
    indexMetadata.setBucket(versionIndexMetadata.getBucket());
    indexMetadata.setInode(versionIndexMetadata.getInode());
    indexMetadata.setCookie(versionIndexMetadata.getCookie());

    // 对于 MinusIndexMetadata 中有而 IndexMetadata 中没有的字段 (shardingStamp)，
    // 这里会保留其默认值 (null)。如果需要从其他地方获取，可以在这里设置。
    indexMetadata.setShardingStamp(StringIncrementer.incrementNumberAfterHyphen(indexMetadata.getSyncStamp()));

    return indexMetadata;
  }

  @Override
  public String toString() {
    return "ObjectMetadata:{" +
      "sysMetaData=" + sysMetaData +
      ", endIndex=" + endIndex +
      ", versionNum='" + versionNum + '\'' +
      ", syncStamp='" + syncStamp + '\'' +
      ", stamp=" + stamp +
      ", storage='" + storage + '\'' +
      ", key='" + key + '\'' +
      ", bucket='" + bucket + '\'' +
      '}';
  }
}
