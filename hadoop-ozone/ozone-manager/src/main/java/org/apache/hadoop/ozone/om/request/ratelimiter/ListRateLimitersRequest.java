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
package org.apache.hadoop.ozone.om.request.ratelimiter;

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.RateLimiterInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.ratelimiter.ListRateLimitersResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * OM client request that lists rate limiters from OM metadata,
 * filtered by volume and bucket.
 */
public class ListRateLimitersRequest extends OMClientRequest {
  public static final Logger LOG = LoggerFactory.getLogger(ListRateLimitersRequest.class);

  public ListRateLimitersRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(
          OzoneManager ozoneManager, long trxnLogIndex) {

    OMRequest omRequest = getOmRequest();
    OzoneManagerProtocolProtos.ListRateLimiterRequest request =
            omRequest.getListRateLimiterRequest();

    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
            OmResponseUtil.getOMResponseBuilder(omRequest);

    OzoneManagerProtocolProtos.Status status = OzoneManagerProtocolProtos.Status.OK;
    String errorMsg = null;

    String filterVolume = request.getVolumeName();
    if (filterVolume.isEmpty()) {
      filterVolume = null;
    }

    String filterBucket = request.getBucketName();
    if (filterBucket.isEmpty()) {
      filterBucket = null;
    }

    List<OzoneManagerProtocolProtos.RateLimiter> result = new ArrayList<>();

    try {
      Table<String, RateLimiterInfo> table =
              metadataManager.getRateLimiterInfoTable();

      try (TableIterator<String,
                    ? extends Table.KeyValue<String, RateLimiterInfo>> iter =
                   table.iterator()) {
        while (iter.hasNext()) {
          Table.KeyValue<String, RateLimiterInfo> kv = iter.next();
          RateLimiterInfo info = kv.getValue();

          if (!matchesFilter(info, filterVolume, filterBucket)) {
            continue;
          }

          result.add(info.toProtobuf());
        }
      }

      LOG.debug("Listed {} rate limiters (volume={}, bucket={})",
              result.size(), filterVolume, filterBucket);

    } catch (IOException e) {
      status = OzoneManagerProtocolProtos.Status.INTERNAL_ERROR;
      errorMsg = "Failed to list rate limiters: " + e.getMessage();
      LOG.error(errorMsg, e);
    }

    OzoneManagerProtocolProtos.ListRateLimiterResponse.Builder listRespBuilder =
            OzoneManagerProtocolProtos.ListRateLimiterResponse.newBuilder()
                    .addAllRateLimiters(result);

    omResponse.setStatus(status)
            .setListRateLimiterResponse(listRespBuilder.build());

    if (errorMsg != null) {
      omResponse.setMessage(errorMsg);
    }

    return new ListRateLimitersResponse(omResponse.build());
  }

  private boolean matchesFilter(RateLimiterInfo info,
                                String filterVolume,
                                String filterBucket) {
    if (filterVolume != null && !filterVolume.equals(info.getVolumeName())) {
      return false;
    }
    if (filterBucket != null && !filterBucket.equals(info.getBucketName())) {
      return false;
    }
    return true;
  }
}
