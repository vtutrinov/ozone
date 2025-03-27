package org.apache.hadoop.ozone.s3.endpoint;

import com.google.common.cache.LoadingCache;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.CompressionType;
import org.apache.hadoop.ozone.s3.OzoneCacheHolder;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test accessing a compressed bucket via S3.
 */
public class TestCompressedBucket {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestCompressedBucket.class);

  public static final String CONTENT = "0123456789";

  private HttpHeaders headers;
  private ObjectEndpoint rest;
  private OzoneClient client;
  private ContainerRequestContext context;


  @BeforeEach
  public void init() throws IOException {
    //GIVEN
    client = new OzoneClientStub();
    createS3Bucket("b1", CompressionType.GZIP, client.getObjectStore());
    OzoneBucket bucket = client.getObjectStore().getS3Bucket("b1");
    OzoneOutputStream keyStream =
        bucket.createKey("key1", CONTENT.getBytes(UTF_8).length);
    keyStream.write(CONTENT.getBytes(UTF_8));
    keyStream.close();

    rest = new ObjectEndpoint();
    rest.setClient(client);
    rest.setOzoneConfiguration(new OzoneConfiguration());
    headers = Mockito.mock(HttpHeaders.class);
    rest.setHeaders(headers);

    OzoneCacheHolder cacheHolder = new OzoneCacheHolder();

    LoadingCache<Pair<String, String>, OzoneKeyDetails> keyDetailsCache =
        cacheHolder.createCache(client, new OzoneConfiguration());
    rest.setKeyDetailsCache(keyDetailsCache);

    context = Mockito.mock(ContainerRequestContext.class);
    Mockito.when(context.getUriInfo()).thenReturn(Mockito.mock(UriInfo.class));
    Mockito.when(context.getUriInfo().getQueryParameters())
        .thenReturn(new MultivaluedHashMap<>());
    rest.setContext(context);
  }


  private void createS3Bucket(
      String bucketName,
      CompressionType compressionType,
      ObjectStore objectStore
  ) throws IOException {
    objectStore.createVolume(HddsClientUtils.getDefaultS3VolumeName(new OzoneConfiguration()));

    OzoneVolume volume = objectStore.getS3Volume();
    // Backwards compatibility:
    // When OM is pre-finalized for the bucket layout feature, it will block
    // the creation of all bucket types except legacy. If OBS bucket creation
    // fails for this reason, retry with legacy bucket layout.
    try {
      BucketArgs.Builder builder = BucketArgs.newBuilder()
          .setBucketLayout(objectStore.getS3BucketLayout());
      if (compressionType != null) {
        builder.setCompressionType(compressionType.getCodecName());
      }

      volume.createBucket(bucketName, builder.build());
    } catch (OMException ex) {
      if (ex.getResult() ==
          OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION) {
        final BucketLayout fallbackLayout = BucketLayout.LEGACY;
        LOG.info("Failed to create S3 bucket with layout {} since OM is " +
                "pre-finalized for bucket layouts. Retrying creation with a " +
                "{} bucket.",
            objectStore.getS3BucketLayout(), fallbackLayout);
        BucketArgs.Builder builder = BucketArgs.newBuilder()
            .setBucketLayout(fallbackLayout);
        if (compressionType != null) {
          builder.setCompressionType(compressionType.getCodecName());
        }
        volume.createBucket(bucketName, builder.build());
      } else {
        throw ex;
      }
    }
  }

  @Test
  public void get() throws IOException, OS3Exception, ExecutionException {
    OS3Exception os3Exception = assertThrows(OS3Exception.class, () ->
        rest.get("b1", "key1", 0, null, 0, null)
    );

    assertTrue(os3Exception.getErrorMessage() != null
        && os3Exception.getErrorMessage().contains("Compressed bucket are not supported in S3"));
  }

  @Test
  public void put() throws IOException, OS3Exception, ExecutionException {
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    assertThrows(OS3Exception.class, () ->
        rest.put("b1", "key1", CONTENT.length(), 1, null, body)
    );
  }

  @Test
  public void testMultipart() throws Exception {
    // Initiate multipart upload
    String uploadID = initiateMultipartUpload(rest, "b1", OzoneConsts.KEY);

    //     Upload parts
    String content = "Multipart Upload 1";
    int partNumber = 1;

    assertThrows(OS3Exception.class, () ->
        uploadPart(rest, OzoneConsts.KEY, uploadID, partNumber, content)
    );
  }

  private String initiateMultipartUpload(ObjectEndpoint endpoint, String bucket, String key) throws IOException,
      OS3Exception {
    Response response = endpoint.initializeMultipartUpload(bucket,
        key);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    String uploadID = multipartUploadInitiateResponse.getUploadID();

    assertEquals(200, response.getStatus());

    return uploadID;
  }

  private CompleteMultipartUploadRequest.Part uploadPart(
      ObjectEndpoint endpoint,
      String key,
      String uploadID,
      int partNumber,
      String content
  ) throws IOException, OS3Exception {
    ByteArrayInputStream body =
        new ByteArrayInputStream(content.getBytes(UTF_8));
    Response response = endpoint.put(OzoneConsts.S3_BUCKET, key, content.length(),
        partNumber, uploadID, body);
    assertEquals(200, response.getStatus());
    assertNotNull(response.getHeaderString(OzoneConsts.ETAG));
    CompleteMultipartUploadRequest.Part part = new CompleteMultipartUploadRequest.Part();
    part.setETag(response.getHeaderString(OzoneConsts.ETAG));
    part.setPartNumber(partNumber);

    return part;
  }
}
