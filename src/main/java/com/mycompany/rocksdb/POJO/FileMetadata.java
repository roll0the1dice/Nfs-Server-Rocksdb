package com.mycompany.rocksdb.POJO;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
public class FileMetadata {
    @JsonProperty("lun")
    private String lun;

    @JsonProperty("vnodeId")
    private String vnodeId;

    @JsonProperty("bucket")
    private String bucket;

    @JsonProperty("fileName")
    private String fileName;

    @JsonProperty("etag")
    private String etag;

    @JsonProperty("size")
    private long size;

    @JsonProperty("offset")
    private List<Long> offset; // 使用 long 类型以防数字过大

    @JsonProperty("len")
    private List<Long> len;

    @JsonProperty("metaKey")
    private String metaKey;

    public String getKey() {
        return fileName;
    }
}
