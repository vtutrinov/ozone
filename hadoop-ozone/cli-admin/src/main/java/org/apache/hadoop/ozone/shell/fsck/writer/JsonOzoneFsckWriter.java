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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Writer;

/**
 * A writer class that outputs Ozone file system checking (fscheck) results in JSON format.
 * Extends {@link AbstractJacksonOzoneFsckWriter} to leverage Jackson for JSON generation.
 */
public final class JsonOzoneFsckWriter extends AbstractJacksonOzoneFsckWriter {
  private JsonOzoneFsckWriter(JsonGenerator jsonGenerator) throws IOException {
    super(jsonGenerator);
  }

  /**
   * Creates a new instance of {@link JsonOzoneFsckWriter} with the given Writer.
   *
   * @param out The Writer to which JSON output will be written.
   * @return A new instance of {@link JsonOzoneFsckWriter}.
   * @throws IOException If an I/O error occurs during the creation of the JSON generator.
   */
  public static JsonOzoneFsckWriter create(Writer out) throws IOException {
    ObjectMapper mapper = new ObjectMapper();

    JsonGenerator jsonGenerator = mapper.createGenerator(out).useDefaultPrettyPrinter();

    return new JsonOzoneFsckWriter(jsonGenerator);
  }
}
