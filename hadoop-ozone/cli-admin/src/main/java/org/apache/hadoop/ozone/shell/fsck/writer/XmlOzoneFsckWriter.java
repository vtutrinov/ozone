/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.shell.fsck.writer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import java.io.IOException;
import java.io.Writer;
import javax.xml.namespace.QName;

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
