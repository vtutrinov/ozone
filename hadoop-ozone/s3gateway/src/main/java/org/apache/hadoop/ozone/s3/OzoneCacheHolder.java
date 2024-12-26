package org.apache.hadoop.ozone.s3;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Caches ozone metadata on the client side.
 */
@ApplicationScoped
public class OzoneCacheHolder {
  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneCacheHolder.class);

  private OzoneClient client;

  @Inject
  private OzoneConfiguration ozoneConfiguration;

  private LoadingCache<Pair<String, String>, OzoneKeyDetails> createCache() {
    CacheLoader<Pair<String, String>, OzoneKeyDetails> loader = new CacheLoader<Pair<String, String>,
        OzoneKeyDetails>() {
      @Override
      public OzoneKeyDetails load(Pair<String, String> key) throws Exception {
        return client.getProxy().getS3KeyDetails(key.getLeft(), key.getRight());
      }
    };
    long timeDuration = ozoneConfiguration.getTimeDuration(
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
    try {
      client = getClient(ozoneConfiguration);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return createCache();
  }

  private OzoneClient getClient(OzoneConfiguration config)
      throws IOException {
    try {
      return OzoneClientCache.getOzoneClientInstance(config);
    } catch (Exception e) {
      // For any other critical errors during object creation throw Internal
      // error.
      LOG.debug("Error during Client Creation: ", e);
      throw e;
    }
  }

}
