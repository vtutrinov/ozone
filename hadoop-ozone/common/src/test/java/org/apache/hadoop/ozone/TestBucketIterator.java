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

package org.apache.hadoop.ozone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test for BucketIterator.
 */
public class TestBucketIterator {

  @Test
  public void testIterator() {
    // given
    Bucket bucket1 = mockBucket("bucket1");
    Bucket bucket2 = mockBucket("bucket2");
    Bucket bucket3 = mockBucket("bucket3");
    Bucket bucket4 = mockBucket("bucket4");

    List<Bucket> bucketsShouldBeHandled = Arrays.asList(bucket1, bucket2, bucket3, bucket4);
    List<Bucket> firstBunchOfBuckets = spy(bucketsShouldBeHandled.subList(0, 2));
    List<Bucket> secondBunchOfBuckets = spy(bucketsShouldBeHandled.subList(2, 4));
    List<Bucket> thirdBunchOfBuckets = spy(Collections.emptyList());

    BucketIterator.BucketListProvider<Bucket> bucketListProvider = mock(BucketIterator.BucketListProvider.class);
    when(bucketListProvider.getList(eq("vol1"), eq(""), eq(""), eq(2), eq(false)))
        .thenReturn(firstBunchOfBuckets);
    when(bucketListProvider.getList(eq("vol1"), eq(""), eq("bucket2"), eq(2), eq(false)))
        .thenReturn(secondBunchOfBuckets);
    when(bucketListProvider.getList(eq("vol1"), eq(""), eq("bucket4"), eq(2), eq(false)))
        .thenReturn(thirdBunchOfBuckets);

    BucketIterator<Bucket> bucketIterator = spy(new BucketIterator<>("vol1", "", "", false, 2, bucketListProvider));
    int iterationIndex = 0;

    // when
    while (bucketIterator.hasNext()) {
      Bucket bucket = bucketIterator.next();
      // then
      assertEquals(bucketsShouldBeHandled.get(iterationIndex), bucket);
      iterationIndex++;
    }

    // and also
    verify(bucketListProvider, times(3)).getList(eq("vol1"), eq(""), anyString(), eq(2), eq(false));
    verify(bucketIterator, times(4)).next();
  }

  private Bucket mockBucket(String bucketName) {
    Bucket bucket = Mockito.mock(Bucket.class);
    when(bucket.getName()).thenReturn(bucketName);
    when(bucket.getVolumeName()).thenReturn("vol1");
    return bucket;
  }

}
