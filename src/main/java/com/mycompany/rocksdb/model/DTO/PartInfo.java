package com.mycompany.rocksdb.model.DTO;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PartInfo {
  private Integer partNumber;
  private Long offset;
  private Integer size;
  private String uploadId;
}
