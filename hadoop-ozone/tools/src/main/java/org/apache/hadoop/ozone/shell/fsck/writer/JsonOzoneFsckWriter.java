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
