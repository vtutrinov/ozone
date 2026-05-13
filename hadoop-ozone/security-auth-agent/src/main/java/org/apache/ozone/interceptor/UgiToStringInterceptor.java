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
package org.apache.ozone.interceptor;

import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import java.util.concurrent.Callable;

/**
 * Intercepts {@code UserGroupInformation.toString()} to display
 * {@code auth:OAUTH} instead of {@code auth:KERBEROS} in logs.
 *
 * <p>The underlying {@code AuthenticationMethod} enum is unchanged
 * (still {@code KERBEROS}) so programmatic auth method checks still
 * pass — only the log/display representation is altered.
 */
public class UgiToStringInterceptor {

  @RuntimeType
  public static String intercept(@SuperCall Callable<String> zuper)
      throws Exception {
    String original = zuper.call();
    if (original == null) {
      return null;
    }
    return original.replace("auth:KERBEROS", "auth:OAUTH");
  }
}
