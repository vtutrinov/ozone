/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.s3;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.TimeUnit;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;

/**
 * Caches ozone metadata on the client side.
 */
@ApplicationScoped
public class OzoneCacheHolder {

  @Inject
  private OzoneConfiguration ozoneConfiguration;

  @Inject
  private OzoneClientCache clientCache;

  public static LoadingCache<Pair<String, String>, OzoneKeyDetails> createCache(
      OzoneClient ozoneClient,
      OzoneConfiguration configuration
  ) {
    CacheLoader<Pair<String, String>, OzoneKeyDetails> loader = new CacheLoader<Pair<String, String>,
        OzoneKeyDetails>() {
      @Override
      public OzoneKeyDetails load(Pair<String, String> key) throws Exception {
        return ozoneClient.getProxy().getS3KeyDetails(key.getLeft(), key.getRight());
      }
    };
    long timeDuration = configuration.getTimeDuration(
        OzoneConfigKeys.OZONE_S3G_KEY_INFO_CACHE_IDLE_LIFETIME,
        OzoneConfigKeys.OZONE_S3G_KEY_INFO_CACHE_IDLE_LIFETIME_DEFAULT,
        TimeUnit.MILLISECONDS);
    return CacheBuilder.newBuilder()
        .weakValues()
        .expireAfterAccess(timeDuration, TimeUnit.MILLISECONDS)
        .build(loader);
  }

  @Produces
  @ApplicationScoped
  public LoadingCache<Pair<String, String>, OzoneKeyDetails> keyDetailsCache() {
    return createCache(clientCache.getClient(), ozoneConfiguration);
  }
}
