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

import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;
import org.apache.ozone.oauth.OAuthTokenManager;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

/**
 * Intercepts calls to {@code UserGroupInformation#doAs(PrivilegedAction)}
 * and validates/refreshes the OAuth token before proceeding.
 */
public class DoAsInterceptor {

  public static Object intercept(@This(optional = true) Object thiz,
      @AllArguments Object[] args,
      @Origin Method method,
      @SuperCall Callable<Object> zuper) throws Exception {

    String ugiName = thiz != null ? thiz.toString() : "unknown";

    // Cache UGI class info for OAuthTokenManager.replaceLoginUser
    // (needed when Thread.currentThread().getContextClassLoader()
    // doesn't return the application classloader, e.g., under hdfs
    // dfs CLI).
    if (thiz != null) {
      OAuthTokenManager.setUgiClass(thiz.getClass());
    } else {
      OAuthTokenManager.setUgiClass(method.getDeclaringClass());
    }

    // Validate/refresh OAuth token for this user
    try {
      OAuthTokenManager.getToken(ugiName);
    } catch (SecurityException e) {
      System.err.println(
          "[SecurityAuthAgent] OAuth check failed for " + ugiName
              + ": " + e.getMessage());
      // Allow the call to proceed — the user may have been
      // authenticated via another mechanism
    }

    return zuper.call();
  }
}
