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

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link ChunkExtensionRejectingConnectionFactory} (CVE-2026-2332).
 */
public class TestChunkExtensionRejectingConnectionFactory {

  private Server server;
  private ServerConnector connector;
  private final AtomicInteger handled = new AtomicInteger();

  @BeforeEach
  public void setUp() throws Exception {
    server = new Server();
    connector = new ServerConnector(server,
        new ChunkExtensionRejectingConnectionFactory(new HttpConfiguration()));
    connector.setHost("127.0.0.1");
    connector.setPort(0);
    server.addConnector(connector);
    server.setHandler(new AbstractHandler() {
      @Override
      public void handle(String target, Request baseRequest,
          HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        handled.incrementAndGet();
        IOUtils.toByteArray(request.getInputStream());
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
      }
    });
    server.start();
  }

  @AfterEach
  public void tearDown() throws Exception {
    server.stop();
  }

  @Test
  public void acceptsChunkedBodyWithoutExtensions() throws Exception {
    String response = send("POST /a HTTP/1.1\r\n"
        + "Host: localhost\r\n"
        + "Transfer-Encoding: chunked\r\n"
        + "Connection: close\r\n"
        + "\r\n"
        + "5\r\nhello\r\n"
        + "0\r\n\r\n");

    assertThat(response).startsWith("HTTP/1.1 200");
    assertEquals(1, handled.get());
  }

  @Test
  public void rejectsChunkExtension() throws Exception {
    String response = send("POST /a HTTP/1.1\r\n"
        + "Host: localhost\r\n"
        + "Transfer-Encoding: chunked\r\n"
        + "Connection: close\r\n"
        + "\r\n"
        + "5;name=value\r\nhello\r\n"
        + "0\r\n\r\n");

    assertThat(response).doesNotStartWith("HTTP/1.1 200");
  }

  @Test
  public void smuggledRequestIsNotProcessed() throws Exception {
    // PoC from GHSA-355h-qmc2-wpwf: the vulnerable parser ends the chunk
    // extension at the CRLF inside the quoted string and then runs the second
    // request found in the body.
    String response = send("POST /a HTTP/1.1\r\n"
        + "Host: localhost\r\n"
        + "Transfer-Encoding: chunked\r\n"
        + "\r\n"
        + "1;a=\"\r\n"
        + "X\r\n"
        + "0\r\n"
        + "\r\n"
        + "GET /smuggled HTTP/1.1\r\n"
        + "Host: localhost\r\n"
        + "Connection: close\r\n"
        + "\r\n");

    assertThat(response).doesNotContain("HTTP/1.1 200");
  }

  private String send(String request) throws IOException {
    try (Socket socket = new Socket("127.0.0.1", connector.getLocalPort())) {
      socket.setSoTimeout(10_000);
      OutputStream out = socket.getOutputStream();
      out.write(request.getBytes(ISO_8859_1));
      out.flush();
      InputStream in = socket.getInputStream();
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      IOUtils.copy(in, buf);
      return buf.toString(ISO_8859_1.name());
    }
  }
}
