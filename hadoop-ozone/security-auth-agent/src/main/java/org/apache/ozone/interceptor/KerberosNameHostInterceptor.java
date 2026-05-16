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

import net.bytebuddy.implementation.bind.annotation.SuperCall;

import java.util.concurrent.Callable;

/**
 * Intercepts
 * {@code org.apache.hadoop.security.authentication.util.KerberosName.getHostName()}
 * to return a non-null fallback when the principal lacks a
 * {@code service/host@REALM} shape.
 *
 * <p>Why: {@code SaslRpcServer}'s KERBEROS constructor throws
 * {@code AccessControlException} when the current UGI's username
 * (e.g. an AM whose login UGI is just {@code "hadoop"} via
 * {@code HADOOP_USER_NAME}) parses to a {@code KerberosName} with
 * a null host part. The constructor never gets to assign
 * {@code protocol}/{@code serverId}, so SASL negotiation aborts.
 * Returning {@code "default"} from this method satisfies the null
 * check; SASL then proceeds to {@code SaslRpcServer.create()} which
 * our existing {@link SaslServerInterceptor} already replaces with
 * {@link org.apache.ozone.oauth.OAuthSaslServer}.
 *
 * <p>This is a no-op for principals that already have a host part:
 * the original {@code getHostName()} returns a real value and we
 * pass it through unchanged.
 */
public class KerberosNameHostInterceptor {

  private static final String FALLBACK_HOST = "default";

  public static String intercept(@SuperCall Callable<String> zuper)
      throws Exception {
    String hostName = zuper.call();
    return hostName != null ? hostName : FALLBACK_HOST;
  }
}
