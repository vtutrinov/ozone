package org.apache.hadoop.ozone.shell.fsck.writer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;

import javax.xml.namespace.QName;
import java.io.IOException;
import java.io.Writer;

/**
 * The implementation of {@link AbstractJacksonOzoneFsckWriter} designed to print a report in the XML format.
 * An output generated using streaming API.
 */
public final class XmlOzoneFsckWriter extends AbstractJacksonOzoneFsckWriter {
  private XmlOzoneFsckWriter(JsonGenerator jsonGenerator) throws IOException {
    super(jsonGenerator);
  }

  /**
   * Creates an instance of {@link XmlOzoneFsckWriter}.
   *
   * @param out an output where the report will be written.
   * @throws IOException will be thrown with either error of creating {@link ToXmlGenerator}
   *    or if {@link ToXmlGenerator} can't write into the provided output.
   */
  public static XmlOzoneFsckWriter create(Writer out) throws IOException {
    XmlMapper xmlMapper = new XmlMapper();

    ToXmlGenerator xmlGenerator = ((ToXmlGenerator) xmlMapper.createGenerator(out).useDefaultPrettyPrinter());
    xmlGenerator.setNextName(new QName("", "report"));

    return new XmlOzoneFsckWriter(xmlGenerator);
  }
}
