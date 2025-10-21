package com.mycompany.rocksdb.POJO;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.File;

import static com.mycompany.rocksdb.utils.MetaKeyUtils.ROCKS_FILE_META_PREFIX;

@Data
@Accessors(chain = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class FileMeta {
    String lun;
    String fileName;
    String etag;
    long size;
    boolean smallFile;
    long[] offset;
    long[] len;
    String metaKey;

    String compression;
    long[] compressBeforeLen;
    long[] compressAfterLen;
    long[] compressState;

    String crypto;
    String secretKey;
    long[] cryptoBeforeLen;
    long[] cryptoAfterLen;
    String cryptoVersion;

    long fileOffset;
    String flushStamp;
    public void setKey(String partKey) {

    }

    @JsonIgnore
    public String getKey() {
        return getKey(fileName);
    }


    public static String getKey(String fileName) {
        return ROCKS_FILE_META_PREFIX + fileName.split(File.separator)[1];
    }
}

