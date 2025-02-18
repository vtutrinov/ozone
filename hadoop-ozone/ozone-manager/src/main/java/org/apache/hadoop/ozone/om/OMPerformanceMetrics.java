/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.ozone.metrics.OzoneMetricsSystem;
import org.apache.hadoop.ozone.metrics.OzoneMutableRate;

/**
 * Including OM performance related metrics.
 */
public class OMPerformanceMetrics {
  private static final String SOURCE_NAME =
      OMPerformanceMetrics.class.getSimpleName();

  public static OMPerformanceMetrics register() {
    MetricsSystem ms = OzoneMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
            "OzoneManager Request Performance",
            new OMPerformanceMetrics());
  }

  public static void unregister() {
    MetricsSystem ms = OzoneMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  @Metric(about = "Overall lookupKey in nanoseconds")
  private OzoneMutableRate lookupLatencyNs;

  @Metric(about = "Read key info from meta in nanoseconds")
  private OzoneMutableRate lookupReadKeyInfoLatencyNs;

  @Metric(about = "Block token generation latency in nanoseconds")
  private OzoneMutableRate lookupGenerateBlockTokenLatencyNs;

  @Metric(about = "Refresh location nanoseconds")
  private OzoneMutableRate lookupRefreshLocationLatencyNs;

  @Metric(about = "ACLs check nanoseconds")
  private OzoneMutableRate lookupAclCheckLatencyNs;

  @Metric(about = "resolveBucketLink latency nanoseconds")
  private OzoneMutableRate lookupResolveBucketLatencyNs;


  @Metric(about = "Overall getKeyInfo in nanoseconds")
  private OzoneMutableRate getKeyInfoLatencyNs;

  @Metric(about = "Read key info from db in getKeyInfo")
  private OzoneMutableRate getKeyInfoReadKeyInfoLatencyNs;

  @Metric(about = "Block token generation latency in getKeyInfo")
  private OzoneMutableRate getKeyInfoGenerateBlockTokenLatencyNs;

  @Metric(about = "Refresh location latency in getKeyInfo")
  private OzoneMutableRate getKeyInfoRefreshLocationLatencyNs;

  @Metric(about = "ACLs check in getKeyInfo")
  private OzoneMutableRate getKeyInfoAclCheckLatencyNs;

  @Metric(about = "Sort datanodes latency in getKeyInfo")
  private OzoneMutableRate getKeyInfoSortDatanodesLatencyNs;

  @Metric(about = "resolveBucketLink latency in getKeyInfo")
  private OzoneMutableRate getKeyInfoResolveBucketLatencyNs;

  @Metric(about = "s3VolumeInfo latency nanoseconds")
  private OzoneMutableRate s3VolumeContextLatencyNs;

  @Metric(about = "Client requests forcing container info cache refresh")
  private OzoneMutableRate forceContainerCacheRefresh;

  @Metric(about = "checkAccess latency in nanoseconds")
  private OzoneMutableRate checkAccessLatencyNs;

  @Metric(about = "listKeys latency in nanoseconds")
  private OzoneMutableRate listKeysLatencyNs;

  @Metric(about = "Validate request latency in nano seconds")
  private OzoneMutableRate validateRequestLatencyNs;

  @Metric(about = "Validate response latency in nano seconds")
  private OzoneMutableRate validateResponseLatencyNs;

  @Metric(about = "PreExecute latency in nano seconds")
  private OzoneMutableRate preExecuteLatencyNs;

  @Metric(about = "Ratis latency in nano seconds")
  private OzoneMutableRate submitToRatisLatencyNs;

  @Metric(about = "Convert om request to ratis request nano seconds")
  private OzoneMutableRate createRatisRequestLatencyNs;

  @Metric(about = "Convert ratis response to om response nano seconds")
  private OzoneMutableRate createOmResoonseLatencyNs;

  @Metric(about = "Ratis local command execution latency in nano seconds")
  private OzoneMutableRate validateAndUpdateCacneLatencyNs;

  @Metric(about = "ACLs check latency in listKeys")
  private OzoneMutableRate listKeysAclCheckLatencyNs;

  @Metric(about = "resolveBucketLink latency in listKeys")
  private OzoneMutableRate listKeysResolveBucketLatencyNs;

  public void addLookupLatency(long latencyInNs) {
    lookupLatencyNs.add(latencyInNs);
  }

  public OzoneMutableRate getLookupRefreshLocationLatencyNs() {
    return lookupRefreshLocationLatencyNs;
  }


  public OzoneMutableRate getLookupGenerateBlockTokenLatencyNs() {
    return lookupGenerateBlockTokenLatencyNs;
  }

  public OzoneMutableRate getLookupReadKeyInfoLatencyNs() {
    return lookupReadKeyInfoLatencyNs;
  }

  public OzoneMutableRate getLookupAclCheckLatencyNs() {
    return lookupAclCheckLatencyNs;
  }

  public void addS3VolumeContextLatencyNs(long latencyInNs) {
    s3VolumeContextLatencyNs.add(latencyInNs);
  }

  public OzoneMutableRate getLookupResolveBucketLatencyNs() {
    return lookupResolveBucketLatencyNs;
  }

  public void addGetKeyInfoLatencyNs(long value) {
    getKeyInfoLatencyNs.add(value);
  }

  public OzoneMutableRate getGetKeyInfoAclCheckLatencyNs() {
    return getKeyInfoAclCheckLatencyNs;
  }

  public OzoneMutableRate getGetKeyInfoGenerateBlockTokenLatencyNs() {
    return getKeyInfoGenerateBlockTokenLatencyNs;
  }

  public OzoneMutableRate getGetKeyInfoReadKeyInfoLatencyNs() {
    return getKeyInfoReadKeyInfoLatencyNs;
  }

  public OzoneMutableRate getGetKeyInfoRefreshLocationLatencyNs() {
    return getKeyInfoRefreshLocationLatencyNs;
  }

  public OzoneMutableRate getGetKeyInfoResolveBucketLatencyNs() {
    return getKeyInfoResolveBucketLatencyNs;
  }

  public OzoneMutableRate getGetKeyInfoSortDatanodesLatencyNs() {
    return getKeyInfoSortDatanodesLatencyNs;
  }

  public void setForceContainerCacheRefresh(boolean value) {
    forceContainerCacheRefresh.add(value ? 1L : 0L);
  }

  public void setCheckAccessLatencyNs(long latencyInNs) {
    checkAccessLatencyNs.add(latencyInNs);
  }

  public void addListKeysLatencyNs(long latencyInNs) {
    listKeysLatencyNs.add(latencyInNs);
  }

  public OzoneMutableRate getValidateRequestLatencyNs() {
    return validateRequestLatencyNs;
  }

  public OzoneMutableRate getValidateResponseLatencyNs() {
    return validateResponseLatencyNs;
  }

  public OzoneMutableRate getPreExecuteLatencyNs() {
    return preExecuteLatencyNs;
  }

  public OzoneMutableRate getSubmitToRatisLatencyNs() {
    return submitToRatisLatencyNs;
  }

  public OzoneMutableRate getCreateRatisRequestLatencyNs() {
    return createRatisRequestLatencyNs;
  }

  public OzoneMutableRate getCreateOmResponseLatencyNs() {
    return createOmResoonseLatencyNs;
  }

  public OzoneMutableRate getValidateAndUpdateCacneLatencyNs() {
    return validateAndUpdateCacneLatencyNs;
  }

  public OzoneMutableRate getListKeysAclCheckLatencyNs() {
    return listKeysAclCheckLatencyNs;
  }

  public OzoneMutableRate getListKeysResolveBucketLatencyNs() {
    return listKeysResolveBucketLatencyNs;
  }
}
