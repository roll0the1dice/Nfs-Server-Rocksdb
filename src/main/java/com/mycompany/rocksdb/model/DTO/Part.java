package com.mycompany.rocksdb.model.DTO;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import lombok.Builder;
import lombok.Data;


@Data
@Builder
@JacksonXmlRootElement(localName = "Part")
public class Part {
  @JacksonXmlProperty(localName = "ETag")
  private String eTag;

  @JacksonXmlProperty(localName = "PartNumber")
  private Integer partNumber; // Use int or long as appropriate

  private String uploadId;
}
