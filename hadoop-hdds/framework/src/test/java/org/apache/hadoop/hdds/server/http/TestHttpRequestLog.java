/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.server.http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.ozone.test.tag.Unhealthy;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.RequestLog;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Testing HttpRequestLog.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestHttpRequestLog {

  @Test
  @Order(1)
  public void testAppenderUndefined() {
    RequestLog requestLog = HttpRequestLog.getRequestLog("test");
    assertNull(requestLog, "RequestLog should be null");
  }

  @Test
  @Order(2)
  @Unhealthy
  public void testAppenderDefined() {
    HttpRequestLogAppender requestLogAppender = new HttpRequestLogAppender(
        "testrequestlog",
        null,
        PatternLayout.createDefaultLayout(),
        true,
        new Property[0]
    );
    LoggerContext lc = (LoggerContext) LogManager.getContext(false);
    lc.getLogger("http.requests.test").addAppender(requestLogAppender);
    RequestLog requestLog = HttpRequestLog.getRequestLog("test");
    lc.getLogger("http.requests.test").removeAppender(requestLogAppender);
    assertNotNull(requestLog, "RequestLog should not be null");
    assertEquals(CustomRequestLog.class, requestLog.getClass(),
        "Class mismatch");
  }

}
