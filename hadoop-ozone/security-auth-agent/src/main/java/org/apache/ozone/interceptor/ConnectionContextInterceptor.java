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
import org.apache.ozone.oauth.OAuthTokenManager;

import java.util.concurrent.Callable;

/**
 * Intercepts {@code Client.ConnectionId.getTicket()} to return the
 * OAuth-authenticated UGI when the original ticket holds the OS user
 * (e.g., "hadoop" in a CLI container).
 *
 * <p>RPC tickets are captured at proxy creation time, often before
 * the agent's interactive login runs. The captured ticket then holds
 * the OS user, leading the server to see a mismatch between the
 * SASL-authenticated user (testuser) and the connection-context user
 * (hadoop), triggering proxy-user authorization that fails.
 *
 * <p>By intercepting {@code getTicket()} we substitute the ticket on
 * read, so the connection context is built with the OAuth UGI and
 * the SASL flow uses the same UGI's username.
 */
public class ConnectionContextInterceptor {

  @RuntimeType
  public static Object getTicket(@SuperCall Callable<Object> zuper)
      throws Exception {
    Object original = zuper.call();
    Object oauthUgi = OAuthTokenManager.getOAuthUgi();
    if (oauthUgi != null && shouldSubstitute(original)) {
      return oauthUgi;
    }
    return original;
  }

  /**
   * Substitute only if the ticket's username doesn't look like a
   * Kerberos principal — i.e., it's an OS user. Service principals
   * (set by LoginInterceptor) already have the correct identity
   * and should pass through unchanged.
   */
  private static boolean shouldSubstitute(Object ugi) {
    if (ugi == null) {
      return true;
    }
    try {
      String name = (String) ugi.getClass()
          .getMethod("getUserName").invoke(ugi);
      if (name == null) {
        return true;
      }
      return name.indexOf('/') < 0 && name.indexOf('@') < 0;
    } catch (Exception e) {
      return false;
    }
  }
}
