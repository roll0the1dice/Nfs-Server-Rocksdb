package com.mycompany.rocksdb.POJO;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SysMetaData {
  @JsonProperty("Content-Length")
  private String contentLength;

  @JsonProperty("Content-Type")
  private String contentType;

  @JsonProperty("owner")
  private String owner;

  // 使用 @JsonProperty 将 JSON 中的 "Last-Modified" 映射到 Java 字段 "lastModified"
  @JsonProperty("Last-Modified")
  private String lastModified;

  // 同样，映射 "ETag"
  @JsonProperty("ETag")
  private String eTag;

  @JsonProperty("displayName")
  private String displayName;

  @Override
  public String toString() {
    return "SysMetaData{" +
      "owner='" + owner + '\'' +
      ", lastModified='" + lastModified + '\'' +
      ", eTag='" + eTag + '\'' +
      ", displayName='" + displayName + '\'' +
      '}';
  }
}
