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

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

import java.io.Serializable;

/**
 * Log4j Appender adapter for HttpRequestLog.
 */
@Plugin(name = "HttpRequestLogAppender", category = "Core", elementType = "appender", printObject = true)
public class HttpRequestLogAppender extends AbstractAppender {

  private String filename;
  private int retainDays;

  protected HttpRequestLogAppender(String name, Filter filter, Layout<? extends Serializable> layout, String filename,
                                   int retainDays) {
    super(name, filter, layout, false, null);
    this.filename = filename;
    this.retainDays = retainDays;
  }

  public void setRetainDays(int retainDays) {
    this.retainDays = retainDays;
  }

  public int getRetainDays() {
    return retainDays;
  }

  public void setFilename(String filename) {
    this.filename = filename;
  }

  public String getFilename() {
    return filename;
  }

  @Override
  public void append(LogEvent event) {

  }

  @PluginFactory
  public static HttpRequestLogAppender createAppender(
      @PluginAttribute("name") String name,
      @PluginAttribute("fileName") String filename,
      @PluginAttribute("retainDays") int retainDays) {
    return new HttpRequestLogAppender(name, null, null, filename, retainDays);
  }

}
