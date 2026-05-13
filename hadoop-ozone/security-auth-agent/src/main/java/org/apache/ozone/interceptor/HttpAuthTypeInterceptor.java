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

/**
 * Intercepts {@code KerberosAuthenticationHandler.getType()} to
 * return {@code "oauth"} so {@code AuthenticationFilter} validates
 * cookies/tokens of type {@code "oauth"}.
 *
 * <p>Lives in its own class (no other static methods) so ByteBuddy
 * has zero ambiguity when binding the override.
 */
public class HttpAuthTypeInterceptor {

  public static String getType() {
    return HttpAuthInterceptor.AUTH_TYPE;
  }
}
