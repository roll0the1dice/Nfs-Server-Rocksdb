package com.mycompany.rocksdb.POJO;

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
public class FileMetadata {
    @JsonProperty("lun")
    private String lun;

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
}
