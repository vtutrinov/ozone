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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.LogConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.slf4j.Log4jLogger;
import org.eclipse.jetty.server.AsyncRequestLogWriter;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.RequestLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RequestLog object for use with Http.
 */
public final class HttpRequestLog {

  public static final Logger LOG =
      LoggerFactory.getLogger(HttpRequestLog.class);
  private static final Map<String, String> SERVER_TO_COMPONENT;

  private HttpRequestLog() {
  }

  static {
    SERVER_TO_COMPONENT = new HashMap<>();
    SERVER_TO_COMPONENT.put("cluster", "resourcemanager");
    SERVER_TO_COMPONENT.put("hdfs", "namenode");
    SERVER_TO_COMPONENT.put("node", "nodemanager");
  }

  public static RequestLog getRequestLog(String name) {

    String lookup = SERVER_TO_COMPONENT.get(name);
    if (lookup != null) {
      name = lookup;
    }
    String loggerName = "http.requests." + name;
    String appenderName = name + "requestlog";
    Logger logger = LoggerFactory.getLogger(loggerName);

    boolean isLog4JLogger;

    try {
      isLog4JLogger = logger instanceof Log4jLogger;
    } catch (NoClassDefFoundError err) {
      // In some dependent projects, log4j may not even be on the classpath at
      // runtime, in which case the above instanceof check will throw
      // NoClassDefFoundError.
      LOG.debug("Could not load Log4JLogger class", err);
      isLog4JLogger = false;
    }

    if (isLog4JLogger) {

      LoggerContext lc = (LoggerContext) LogManager.getContext(false);

      Appender appender;

      try {
        appender = lc.getConfiguration().getAppender(appenderName);
      } catch (LogConfigurationException e) {
        LOG.warn("Http request log for {} could not be created", loggerName);
        throw e;
      }

      if (appender == null) {
        LOG.info("Http request log for {} is not defined", loggerName);
        return null;
      }

      if (appender instanceof HttpRequestLogAppender) {
        HttpRequestLogAppender requestLogAppender
            = (HttpRequestLogAppender) appender;
        AsyncRequestLogWriter logWriter = new AsyncRequestLogWriter();
        logWriter.setFilename(requestLogAppender.getFilename());
        logWriter.setRetainDays(requestLogAppender.getRetainDays());
        return new CustomRequestLog(logWriter,
            CustomRequestLog.EXTENDED_NCSA_FORMAT);
      } else {
        LOG.warn("Jetty request log for {} was of the wrong class", loggerName);
        return null;
      }
    } else {
      LOG.warn("Jetty request log can only be enabled using Log4j");
      return null;
    }
  }
}
