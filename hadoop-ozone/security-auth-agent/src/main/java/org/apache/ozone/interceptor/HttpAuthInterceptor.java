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

import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;

import java.lang.reflect.Method;

/**
 * Intercepts {@code KerberosAuthenticationHandler.init(Properties)}
 * to be a no-op since we have no keytab on disk.
 */
public class HttpAuthInterceptor {

  /** The auth type this handler advertises after the agent loads. */
  public static final String AUTH_TYPE = "oauth";

  @RuntimeType
  public static void interceptInit(@Origin Method method) {
    System.out.println(
        "[SecurityAuthAgent] Skipping " + method.getDeclaringClass()
            .getSimpleName() + ".init() — no keytab needed (OAuth)");
  }
}
