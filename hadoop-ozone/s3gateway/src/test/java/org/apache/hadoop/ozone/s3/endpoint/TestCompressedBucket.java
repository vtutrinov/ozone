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

package org.apache.hadoop.ozone.s3.endpoint;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import javax.ws.rs.core.HttpHeaders;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.CompressionType;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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


  @BeforeEach
  public void init() throws IOException {
    //GIVEN
    client = new OzoneClientStub();
    // creates the S3 volume in the stub
    client.getObjectStore().createS3Bucket("plain");
    createS3Bucket("b1", CompressionType.GZIP, client.getObjectStore());
    OzoneBucket bucket = client.getObjectStore().getS3Bucket("b1");
    OzoneOutputStream keyStream =
        bucket.createKey("key1", CONTENT.getBytes(UTF_8).length);
    keyStream.write(CONTENT.getBytes(UTF_8));
    keyStream.close();

    headers = Mockito.mock(HttpHeaders.class);
    Mockito.when(headers.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("UNSIGNED-PAYLOAD");
    rest = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(client)
        .setHeaders(headers)
        .build();
  }


  private void createS3Bucket(
      String bucketName,
      CompressionType compressionType,
      ObjectStore objectStore
  ) throws IOException {
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
  public void get() {
    OS3Exception ex = assertThrows(OS3Exception.class, () -> EndpointTestUtils.get(rest, "b1", "key1"));
    assertEquals(S3ErrorTable.NOT_IMPLEMENTED.getCode(), ex.getCode());
  }

  @Test
  public void put() {
    OS3Exception ex = assertThrows(OS3Exception.class, () -> EndpointTestUtils.put(rest, "b1", "key1", CONTENT));
    assertEquals(S3ErrorTable.NOT_IMPLEMENTED.getCode(), ex.getCode());
  }

  @Test
  public void testMultipart() throws Exception {
    String uploadID = EndpointTestUtils.initiateMultipartUpload(rest, "b1", OzoneConsts.KEY);

    OS3Exception ex = assertThrows(OS3Exception.class, () ->
        EndpointTestUtils.uploadPart(rest, "b1", OzoneConsts.KEY, 1, uploadID, "Multipart Upload 1"));
    assertEquals(S3ErrorTable.NOT_IMPLEMENTED.getCode(), ex.getCode());
  }
}
