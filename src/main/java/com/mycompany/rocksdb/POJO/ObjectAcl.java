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
public class ObjectAcl {
    @JsonProperty("acl")
    private int acl;

    @JsonProperty("owner")
    private String owner;
}
