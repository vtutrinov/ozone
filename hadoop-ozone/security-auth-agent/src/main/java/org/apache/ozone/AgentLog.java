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
package org.apache.ozone;

/**
 * Tiny level-aware logger for {@code [SecurityAuthAgent]} stdout
 * messages. Deliberately not SLF4J/log4j:
 * <ul>
 *   <li>the agent runs on every JVM in the cluster (often on the
 *       boot classpath, before the host's logging stack is up);
 *   <li>a shaded logging framework would multiply classloader
 *       complexity for what's diagnostic output, not application
 *       logs;
 *   <li>stdout/stderr are visible in container logs regardless of
 *       how the host configures logging.
 * </ul>
 *
 * <p>Level resolution at startup:
 * <ol>
 *   <li>{@code -javaagent:...=auth-log-level=LEVEL} (parsed by
 *       {@link org.apache.ozone.config.AgentArgsParser} and pushed
 *       in via {@link #setLevel(String)});
 *   <li>env var {@code OZONE_AGENT_LOG_LEVEL};
 *   <li>default {@code INFO}.
 * </ol>
 *
 * <p>{@code ERROR} and above go to {@code System.err}; everything
 * else to {@code System.out}.
 */
public final class AgentLog {

  /** Levels in increasing verbosity. */
  public enum Level {
    OFF, ERROR, WARN, INFO, DEBUG
  }

  private static final String PREFIX = "[SecurityAuthAgent] ";
  private static final String ENV_VAR = "OZONE_AGENT_LOG_LEVEL";

  private static volatile Level current = readInitialLevel();

  private AgentLog() {
  }

  /** Override the level (e.g. from parsed agent args). */
  public static void setLevel(String name) {
    if (name == null || name.isEmpty()) {
      return;
    }
    try {
      current = Level.valueOf(name.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      // ignore — keep previous
    }
  }

  public static Level getLevel() {
    return current;
  }

  public static boolean isEnabled(Level level) {
    return current.ordinal() >= level.ordinal();
  }

  public static void error(String msg) {
    if (isEnabled(Level.ERROR)) {
      System.err.println(PREFIX + msg);
    }
  }

  public static void error(String msg, Throwable t) {
    if (isEnabled(Level.ERROR)) {
      System.err.println(PREFIX + msg);
      if (t != null) {
        t.printStackTrace();
      }
    }
  }

  public static void warn(String msg) {
    if (isEnabled(Level.WARN)) {
      System.err.println(PREFIX + msg);
    }
  }

  public static void info(String msg) {
    if (isEnabled(Level.INFO)) {
      System.out.println(PREFIX + msg);
    }
  }

  public static void debug(String msg) {
    if (isEnabled(Level.DEBUG)) {
      System.out.println(PREFIX + msg);
    }
  }

  private static Level readInitialLevel() {
    String env = System.getenv(ENV_VAR);
    if (env == null || env.isEmpty()) {
      return Level.INFO;
    }
    try {
      return Level.valueOf(env.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      return Level.INFO;
    }
  }
}
