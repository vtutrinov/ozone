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
 * Iterator for listing volumes with an option to provide a data source for the list.
 * @param <T> type of iterated objects
 */
public class VolumeIterator<T extends Volume> implements Iterator<T> {

  private String user;
  private String volPrefix;
  private int listSize;

  private VolumeListProvider volumeListProvider;
  private Iterator<T> currentIterator;
  private T currentValue;

  public VolumeIterator(String volPrefix, String prevVolume, String user, int listSize,
                        VolumeListProvider<T> volumeListProvider) {
    this.volPrefix = volPrefix;
    this.user = user;
    this.volumeListProvider = volumeListProvider;
    this.currentValue = null;
    this.listSize = listSize;
    this.currentIterator = getNextListOfVolumes(prevVolume).iterator();
  }

  @Override
  public boolean hasNext() {
    if (!currentIterator.hasNext() && currentValue != null) {
      currentIterator = getNextListOfVolumes(currentValue.getName()).iterator();
    }
    return currentIterator.hasNext();
  }

  @Override
  public T next() {
    if (hasNext()) {
      currentValue = currentIterator.next();
      return currentValue;
    }
    throw new NoSuchElementException();
  }

  /**
   * Returns the next set of volume list using proxy.
   * @param prevVolume previous volume, this will be excluded from the result
   * @return {@code List<OzoneVolume>}
   */
  private List<T> getNextListOfVolumes(String prevVolume) {
    try {
      return (List<T>) volumeListProvider.getList(user, volPrefix, prevVolume, listSize);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * Functional interface for providing list of volumes.
   * @param <T> type of volume items in provided list
   */
  @FunctionalInterface
  public interface VolumeListProvider<T> {

    List<T> getList(String user, String volPrefix, String prevVolume, int listSize);

  }

}
