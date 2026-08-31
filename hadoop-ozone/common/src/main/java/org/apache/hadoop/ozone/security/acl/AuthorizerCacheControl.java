/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.security.acl;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Optional capability of an {@link IAccessAuthorizer} whose authorization
 * decisions are backed by a cache of externally managed policies (e.g. the
 * Apache Ranger ozone plugin). Allows OzoneManager to control the cache
 * lifecycle on behalf of cluster administrators.
 *
 * All state controlled through this interface is local to the OzoneManager
 * process hosting the authorizer instance.
 *
 * Implementations may be loaded in a child classloader; only JDK types
 * cross this interface so that both sides resolve identical classes.
 */
@InterfaceAudience.LimitedPrivate({"Ranger"})
@InterfaceStability.Evolving
public interface AuthorizerCacheControl {

  /**
   * Invalidate the whole policy cache. Until the authorizer successfully
   * re-syncs with its policy source, all access checks are denied.
   */
  void invalidateCache() throws IOException;

  /**
   * Invalidate a single cached policy by name. The policy is removed from
   * the cached policy set until the next successful sync with the policy
   * source restores the authoritative state.
   *
   * @return true if a policy with the given name was found and removed.
   */
  boolean invalidateCachedPolicy(String policyName) throws IOException;

  /**
   * Invalidate a single cached role by name. The role is removed from the
   * cached role set until the next successful sync with the policy source
   * restores the authoritative state.
   *
   * @return true if a role with the given name was found and removed.
   */
  boolean invalidateCachedRole(String roleName) throws IOException;

  /**
   * Extend the cache validity deadline to now + extensionMillis, evaluated
   * on the local clock of this node. Used to keep serving cached policies
   * through a planned outage of the policy source beyond the configured
   * expiry interval.
   */
  void extendCacheValidity(long extensionMillis) throws IOException;

  /**
   * @return a JSON document describing the current cache state (state,
   * last successful contact with the policy source, effective deadline,
   * policy/role versions, denial counters).
   */
  String getCacheStatus() throws IOException;
}
