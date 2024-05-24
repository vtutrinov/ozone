/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.log;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.slf4j.event.LoggingEvent;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * LogVerificationAppender is a custom Log4j2 Appender that captures and stores LogEvents
 * for verification and testing purposes.
 * It extends the AbstractAppender class.
 */
public class LogVerificationAppender extends AbstractAppender {
  private static final String NAME = "LogVerificationAppender";

  private final List<LogEvent> log = new ArrayList<>();

  protected LogVerificationAppender(String name,
                                    Filter filter,
                                    Layout<? extends Serializable> layout,
                                    boolean ignoreExceptions,
                                    Property[] properties) {
    super(name, filter, layout, ignoreExceptions, properties);
  }

  public LogVerificationAppender() {
    this(
        NAME,
        null,
        PatternLayout.createDefaultLayout(),
        true,
        new Property[0]
    );
  }

  @Override
  public void append(LogEvent event) {
    this.log.add(event);
  }

  public void close() {
  }

  public List<LogEvent> getLog() {
    return new ArrayList<>(this.log);
  }

  public int countExceptionsWithMessage(String text) {
    int count = 0;

    for (LogEvent logEvent : this.getLog()) {
      LoggingEvent e = (LoggingEvent) logEvent;
      Throwable t = e.getThrowable();
      if (t != null) {
        String m = t.getMessage();
        if (m.contains(text)) {
          ++count;
        }
      }
    }

    return count;
  }

  public int countLinesWithMessage(String text) {
    int count = 0;

    for (LogEvent logEvent : this.getLog()) {
      LoggingEvent e = (LoggingEvent) logEvent;
      String msg = e.getMessage();
      if (msg != null && msg.contains(text)) {
        ++count;
      }
    }

    return count;
  }
}
