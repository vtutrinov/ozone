/**
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

package org.apache.hadoop.ozone.logging.log4j;

import org.apache.logging.slf4j.Log4jLoggerFactory;
import org.apache.logging.slf4j.Log4jMDCAdapter;
import org.apache.logging.slf4j.Log4jMarkerFactory;
import org.slf4j.ILoggerFactory;
import org.slf4j.IMarkerFactory;
import org.slf4j.spi.MDCAdapter;

/**
 * SLF4J service provider for Log4j.
 */
public class Log4JSLF4JServiceProvider implements org.slf4j.spi.SLF4JServiceProvider {

  public static final String REQUESTED_API_VERSION = "2.0.13";

  private ILoggerFactory loggerFactory;
  private Log4jMarkerFactory markerFactory;
  private MDCAdapter mdcAdapter;

  @Override
  public ILoggerFactory getLoggerFactory() {
    return loggerFactory;
  }

  @Override
  public IMarkerFactory getMarkerFactory() {
    return markerFactory;
  }

  @Override
  public MDCAdapter getMDCAdapter() {
    return mdcAdapter;
  }

  @Override
  public String getRequestedApiVersion() {
    return REQUESTED_API_VERSION;
  }

  @Override
  public void initialize() {
    markerFactory = new Log4jMarkerFactory();
    loggerFactory = new Log4jLoggerFactory(new Log4jMarkerFactory());
    mdcAdapter = new Log4jMDCAdapter();
  }

}
