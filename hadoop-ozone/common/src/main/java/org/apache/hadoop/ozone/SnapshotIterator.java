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
 * Iterator for listing snapshots with option to provide a remote source of the items.
 * @param <T> type of iterated objects
 */
public class SnapshotIterator<T extends Snapshot> implements Iterator<T> {

  private final String volumeName;
  private final String bucketName;
  private final String snapshotPrefix;
  private final int listCacheSize;
  private final SnapshotListProvider<T> snapshotListProvider;

  private T currentSnapshot;

  private Iterator<T> currentIterator;

  public SnapshotIterator(String volumeName, String bucketName, String snapshotPrefix, String prevSnapshot,
                          int listSize, SnapshotListProvider<T> snapshotListProvider) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.snapshotPrefix = snapshotPrefix;
    this.listCacheSize = listSize;
    this.snapshotListProvider = snapshotListProvider;
    this.currentIterator = getNextListOfSnapshots(prevSnapshot).iterator();
  }

  @Override
  public boolean hasNext() {
    if (!currentIterator.hasNext() && currentSnapshot != null) {
      List<T> nextListOfSnapshots = getNextListOfSnapshots(currentSnapshot.getName());
      currentIterator = nextListOfSnapshots.iterator();
      if (nextListOfSnapshots.isEmpty() || currentSnapshot.getName().equals(nextListOfSnapshots.get(0).getName())) {
        return false;
      }
    }
    return currentIterator.hasNext();
  }

  @Override
  public T next() {
    if (hasNext()) {
      currentSnapshot = currentIterator.next();
      return currentSnapshot;
    }
    throw new NoSuchElementException();
  }

  private List<T> getNextListOfSnapshots(String startSnapshot) {
    try {
      return snapshotListProvider.getList(volumeName, bucketName, snapshotPrefix, startSnapshot, listCacheSize);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Functional interface for providing a source of list of snapshots.
   * @param <T> type of iterated objects
   */
  @FunctionalInterface
  public interface SnapshotListProvider<T> {
    List<T> getList(String volumeName, String bucketName, String snapshotPrefix, String prevSnapshot,
                           int listSize);
  }

}
