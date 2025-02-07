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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Iterator for listing buckets.
 */
public class BucketIterator<T extends Bucket> implements Iterator<T> {

  private String volumeName;
  private String bucketPrefix = null;
  private boolean hasSnapshot;
  private int listSize;
  private Iterator<T> currentIterator;
  private T currentBucket;
  private BucketListProvider<T> bucketListProvider;

  public BucketIterator(String volumeName, String bucketPrefix, String prevBucket, boolean hasSnapshot, int listSize,
                        BucketListProvider<T> bucketListProvider) {
    this.volumeName = volumeName;
    this.bucketPrefix = bucketPrefix;
    this.hasSnapshot = hasSnapshot;
    this.bucketListProvider = bucketListProvider;
    this.listSize = listSize;
    this.currentIterator = getNextListOfBuckets(prevBucket).iterator();
  }

  @Override
  public boolean hasNext() {
    if (!currentIterator.hasNext() && currentBucket != null) {
      currentIterator = getNextListOfBuckets(currentBucket.getName()).iterator();
    }
    return currentIterator.hasNext();
  }

  @Override
  public T next() {
    if (hasNext()) {
      currentBucket = currentIterator.next();
      return currentBucket;
    }
    throw new NoSuchElementException();
  }

  private List<T> getNextListOfBuckets(String prevBucket) {
    try {
      return bucketListProvider.getList(volumeName, bucketPrefix, prevBucket, listSize, hasSnapshot);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Functional interface for providing list of buckets.
   */
  @FunctionalInterface
  public interface BucketListProvider<T> {
    List<T> getList(String volumeName, String bucketPrefix, String prevBucket, int listSize,
                        boolean hasSnapshot);
  }

}
