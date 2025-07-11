package com.mycompany.rocksdb.POJO;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
public class LatestIndexMetadata {
    @JsonProperty("sysMetaData")
    private String sysMetaData;

    @JsonProperty("endIndex")
    private long endIndex;

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
}
