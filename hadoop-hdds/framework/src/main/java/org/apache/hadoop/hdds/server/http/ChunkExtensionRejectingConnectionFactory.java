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

package org.apache.hadoop.hdds.server.http;

import org.eclipse.jetty.http.BadMessageException;
import org.eclipse.jetty.http.HttpCompliance;
import org.eclipse.jetty.http.HttpParser;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnection;
import org.eclipse.jetty.server.HttpConnectionFactory;

/**
 * SDP: HTTP/1.1 connection factory whose parser rejects chunk extensions
 * ({@code <size>;name=value} in a chunked request body).
 * <p>
 * Mitigates CVE-2026-2332 (request smuggling): the Jetty 9.4 parser ends a
 * chunk extension at the first line break, even inside a quoted string, so a
 * front proxy that honours the quoting frames the body differently and lets a
 * second request be smuggled on a shared connection. The fix is only in
 * commercial Jetty 9.4.60+ builds. No S3/Hadoop client sends chunk extensions,
 * so the request is answered with 400 and the connection is closed.
 */
public class ChunkExtensionRejectingConnectionFactory
    extends HttpConnectionFactory {

  public ChunkExtensionRejectingConnectionFactory(HttpConfiguration config) {
    super(config);
  }

  @Override
  public Connection newConnection(Connector connector, EndPoint endPoint) {
    HttpConnection conn = new HttpConnection(getHttpConfiguration(), connector,
        endPoint, getHttpCompliance(), isRecordHttpComplianceViolations()) {
      @Override
      protected HttpParser newHttpParser(HttpCompliance compliance) {
        return new ChunkExtensionRejectingParser(newRequestHandler(),
            getHttpConfiguration().getRequestHeaderSize(), compliance);
      }
    };
    return configure(conn, connector, endPoint);
  }

  /**
   * Parser failing as soon as it starts reading a chunk extension.
   */
  static class ChunkExtensionRejectingParser extends HttpParser {

    ChunkExtensionRejectingParser(RequestHandler handler, int maxHeaderBytes,
        HttpCompliance compliance) {
      super(handler, maxHeaderBytes, compliance);
    }

    @Override
    protected void setState(State state) {
      if (state == State.CHUNK_PARAMS) {
        // Caught by parseNext(): the request fails with 400 and the parser
        // moves to CLOSE, so the rest of the connection is never parsed.
        throw new BadMessageException(HttpStatus.BAD_REQUEST_400,
            "Chunk extensions are not supported");
      }
      super.setState(state);
    }
  }
}
