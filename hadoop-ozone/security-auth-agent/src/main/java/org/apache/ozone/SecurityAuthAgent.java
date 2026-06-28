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

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.implementation.MethodDelegation;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.nameMatches;
import static net.bytebuddy.matcher.ElementMatchers.nameStartsWith;
import static net.bytebuddy.matcher.ElementMatchers.none;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import org.apache.ozone.config.AgentArgsParser;
import org.apache.ozone.config.AgentConfig;
import org.apache.ozone.interceptor.DoAsInterceptor;
import org.apache.ozone.interceptor.ConnectionContextInterceptor;
import org.apache.ozone.interceptor.HttpAuthInterceptor;
import org.apache.ozone.interceptor.HttpAuthTypeInterceptor;
import org.apache.ozone.interceptor.HttpOidcInterceptor;
import org.apache.ozone.interceptor.KerberosAuthenticatorInterceptor;
import org.apache.ozone.interceptor.KerberosNameHostInterceptor;
import org.apache.ozone.interceptor.LoginInterceptor;
import org.apache.ozone.interceptor.SaslClientInterceptor;
import org.apache.ozone.interceptor.SaslServerInterceptor;
import org.apache.ozone.interceptor.UgiToStringInterceptor;
import org.apache.ozone.provider.AuthDataProvider;

import java.lang.instrument.Instrumentation;
import java.util.ServiceLoader;

/**
 * Java agent that instruments Hadoop's
 * {@code UserGroupInformation} to replace Kerberos authentication
 * with OAuth token-based authentication.
 *
 * <p>Usage: {@code -javaagent:security-auth-agent.jar=--auth-data-provider=env}
 */
public class SecurityAuthAgent {

  private static volatile AuthDataProvider provider;
  private static volatile AgentConfig agentConfig;

  public static void premain(String agentArgs, Instrumentation inst) {
    init(agentArgs, inst);
  }

  public static void agentmain(String agentArgs, Instrumentation inst) {
    init(agentArgs, inst);
  }

  private static void init(String agentArgs, Instrumentation inst) {
    try {
      agentConfig = AgentArgsParser.parse(agentArgs);
      AgentLog.setLevel(agentConfig.getLogLevel());
      AgentLog.info("Config: " + agentConfig);
      resolveProvider();
      registerSaslProvider();
      installAgent(inst);
      prewarmAuth();
    } catch (Throwable t) {
      AgentLog.error("Failed to initialize!", t);
    }
  }

  /**
   * Register a JCA SASL provider so every {@code Sasl.createSasl*}
   * caller — including Hive's thrift {@code TSaslClientTransport}
   * / {@code TSaslServerTransport} — picks up the OAuth factories.
   * Inserted at position 1 so we're tried before the JDK default
   * GSSAPI provider. Idempotent: if a provider with the same name
   * is already present (e.g. agent loaded twice), we leave it alone.
   */
  private static void registerSaslProvider() {
    try {
      if (java.security.Security.getProvider(
          org.apache.ozone.oauth.OAuthSaslProvider.NAME) == null) {
        java.security.Security.insertProviderAt(
            new org.apache.ozone.oauth.OAuthSaslProvider(), 1);
        AgentLog.debug(
            "Registered OAuth SASL provider for GSSAPI");
      }
    } catch (Throwable t) {
      AgentLog.warn(
          "Failed to register OAuth SASL provider: " + t.getMessage());
    }
  }

  /**
   * Obtain an OAuth token at premain time so {@code loginUser} is
   * replaced with the OAuth-authenticated UGI before any application
   * code runs. Without this, classes like {@code Job} capture
   * {@code UGI.getCurrentUser()} (still the OS user) in their
   * constructor, and later {@code job.submit()} runs inside a
   * {@code doAs} of that captured user — staging dir gets resolved
   * as {@code /user/$OS_USER/.staging} instead of
   * {@code /user/$OAUTH_USER/.staging}.
   */
  private static void prewarmAuth() {
    if (provider == null) {
      return;
    }
    try {
      org.apache.ozone.oauth.OAuthTokenManager.getToken("default");
    } catch (Throwable t) {
      AgentLog.warn("Pre-warm auth failed: " + t.getMessage()
          + " (lazy path will retry)");
    }
  }

  private static void resolveProvider() {
    String name = agentConfig.getProviderName();
    ServiceLoader<AuthDataProvider> loader =
        ServiceLoader.load(AuthDataProvider.class);
    for (AuthDataProvider candidate : loader) {
      if (candidate.getName().equals(name)) {
        candidate.init(agentConfig);
        provider = candidate;
        AgentLog.info("Using provider: " + name);
        return;
      }
    }
    AgentLog.error("No provider found for: " + name);
  }

  private static void installAgent(Instrumentation inst) {
    new AgentBuilder.Default()
        .ignore(nameStartsWith("net.bytebuddy."))
        .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
        .ignore(none())
        .type(named(
            "org.apache.hadoop.security.UserGroupInformation"))
        .transform((builder, typeDescription, classLoader, module,
                    protectionDomain) ->
            builder
                .method(named("doAs").and(takesArguments(1)))
                .intercept(MethodDelegation.to(DoAsInterceptor.class))
                .method(named("loginUserFromKeytab")
                    .and(takesArguments(2)))
                .intercept(
                    MethodDelegation.to(LoginInterceptor.class))
                .method(named("loginUserFromKeytabAndReturnUGI")
                    .and(takesArguments(2)))
                .intercept(
                    MethodDelegation.to(LoginInterceptor.class))
                .method(named("toString").and(takesArguments(0)))
                .intercept(
                    MethodDelegation.to(UgiToStringInterceptor.class))
        )
        // Match both the standard Hadoop classes (used by YARN) and
        // Ozone's relocated copies in the *_ packages.
        .type(nameMatches(
            "org\\.apache\\.hadoop\\.security_?\\.SaslRpcClient"))
        .transform((builder, typeDescription, classLoader, module,
                    protectionDomain) ->
            builder
                .method(named("createSaslClient")
                    .and(takesArguments(1)))
                .intercept(
                    MethodDelegation.to(SaslClientInterceptor.class))
        )
        .type(nameMatches(
            "org\\.apache\\.hadoop\\.security_?\\.SaslRpcServer"))
        .transform((builder, typeDescription, classLoader, module,
                    protectionDomain) ->
            builder
                .method(named("create")
                    .and(takesArguments(3)))
                .intercept(
                    MethodDelegation.to(SaslServerInterceptor.class))
        )
        .type(named("org.apache.hadoop.security.authentication.server"
            + ".KerberosAuthenticationHandler"))
        .transform((builder, typeDescription, classLoader, module,
                    protectionDomain) ->
            builder
                .method(named("init").and(takesArguments(1)))
                .intercept(
                    MethodDelegation.to(HttpAuthInterceptor.class))
                .method(named("getType").and(takesArguments(0)))
                .intercept(
                    MethodDelegation.to(HttpAuthTypeInterceptor.class))
                .method(named("authenticate").and(takesArguments(2)))
                .intercept(
                    MethodDelegation.to(HttpOidcInterceptor.class))
        )
        .type(named("org.apache.hadoop.security.authentication.client"
            + ".KerberosAuthenticator"))
        .transform((builder, typeDescription, classLoader, module,
                    protectionDomain) ->
            builder
                .method(named("authenticate").and(takesArguments(2)))
                .intercept(MethodDelegation.to(
                    KerberosAuthenticatorInterceptor.class))
        )
        // KerberosName.getHostName() returns null when the principal
        // has no host part (e.g. an AM whose login UGI is just
        // "hadoop"). SaslRpcServer's KERBEROS constructor throws on
        // that, aborting SASL before our SaslServerInterceptor can
        // swap in the OAuth server. Return a fallback host instead.
        .type(named("org.apache.hadoop.security.authentication.util"
            + ".KerberosName"))
        .transform((builder, typeDescription, classLoader, module,
                    protectionDomain) ->
            builder
                .method(named("getHostName").and(takesArguments(0)))
                .intercept(MethodDelegation.to(
                    KerberosNameHostInterceptor.class))
        )
        .type(nameMatches(
            "org\\.apache\\.hadoop\\.ipc_?\\.Client\\$ConnectionId"))
        .transform((builder, typeDescription, classLoader, module,
                    protectionDomain) ->
            builder
                .method(named("getTicket").and(takesArguments(0)))
                .intercept(MethodDelegation.to(
                    ConnectionContextInterceptor.class))
        )
        .installOn(inst);
    AgentLog.info("Installed Hadoop security auth agent");
  }

  public static AuthDataProvider getProvider() {
    return provider;
  }

  public static AgentConfig getAgentConfig() {
    return agentConfig;
  }
}
