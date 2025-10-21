package com.mycompany.rocksdb.model.DTO;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import lombok.Data;

import java.util.List;

@Data
@JacksonXmlRootElement(localName = "CompleteMultipartUpload", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
public class CompleteMultipartUpload {
  // This annotation pair is key for handling repeating elements without a specific wrapper tag (like <Parts>)
  @JacksonXmlElementWrapper(useWrapping = false) // Tells Jackson there isn't an extra <Parts> wrapper element
  @JacksonXmlProperty(localName = "Part")       // Specifies the name of the repeating element itself
  private List<Part> parts;
}
