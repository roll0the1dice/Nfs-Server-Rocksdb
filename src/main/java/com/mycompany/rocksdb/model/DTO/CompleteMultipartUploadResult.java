package com.mycompany.rocksdb.model.DTO;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import lombok.Data;


@Data
@JacksonXmlRootElement(localName = "InitiateMultipartUploadResult", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
public class CompleteMultipartUploadResult {
  @JacksonXmlProperty(localName = "Bucket")
  private String bucket;

  @JacksonXmlProperty(localName = "Key")
  private String key;

  @JacksonXmlProperty(localName = "ETag")
  private String eTag;
}
