/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ozone.test;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.slf4j.Log4jLogger;
import org.slf4j.Logger;

import java.lang.reflect.Field;

/**
 * Capture Log4j logs.
 */
class Log4jCapturer extends GenericTestUtils.LogCapturer {
  private final Appender appender;
  private final Logger logger;

  private static Layout getDefaultLayout() {
    LoggerContext.getContext().getRootLogger().setLevel(Level.ALL);
    Appender defaultAppender = LoggerContext.getContext().getRootLogger().getAppenders().get("stdout");
    if (defaultAppender == null) {
      defaultAppender = LoggerContext.getContext().getRootLogger().getAppenders().get("console");
    }

    return defaultAppender == null
        ? PatternLayout.newBuilder().build()
        : defaultAppender.getLayout();
  }

  Log4jCapturer(org.slf4j.Logger logger) {
    this(logger, getDefaultLayout());
  }

  Log4jCapturer(Logger logger, Layout layout) {
    this.logger = logger;
    this.appender = WriterAppender.newBuilder().setName(logger.getName()).setLayout(layout).setTarget(writer()).build();
    Log4jLogger log4jLogger = (Log4jLogger) logger;
    try {
      Field loggerField = log4jLogger.getClass().getDeclaredField("logger");
      loggerField.setAccessible(true);
      org.apache.logging.log4j.core.Logger internalLog4jLogger =
          (org.apache.logging.log4j.core.Logger) loggerField.get(log4jLogger);
      internalLog4jLogger.addAppender(appender);
      internalLog4jLogger.setLevel(Level.ALL);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public void stopCapturing() {
    try {
      Log4jLogger log4jLogger = (Log4jLogger) logger;
      Field internalLoggerField = log4jLogger.getClass().getDeclaredField("logger");
      internalLoggerField.setAccessible(true);
      org.apache.logging.log4j.core.Logger internalLog4jLogger =
          (org.apache.logging.log4j.core.Logger) internalLoggerField.get(log4jLogger);
      internalLog4jLogger.removeAppender(appender);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
