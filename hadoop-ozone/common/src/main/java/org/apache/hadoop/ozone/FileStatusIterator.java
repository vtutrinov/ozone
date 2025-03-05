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
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatusLight;

/**
 * Iterator for listing files statuses with an option to provide a data source.
 */
public class FileStatusIterator implements Iterator<OzoneFileStatusLight> {

  private OmKeyArgs omKeyArgs;
  private int listSize;
  private String startPath;
  private Iterator<OzoneFileStatusLight> currentIterator;
  private OzoneFileStatusLight currentFileStatus;
  private FileStatusListProvider fileStatusListProvider;
  private boolean singleDirectoryIterator;

  public FileStatusIterator(OmKeyArgs omKeyArgs, int listSize, String startPath,
                            FileStatusListProvider fileStatusListProvider) {
    this.omKeyArgs = omKeyArgs;
    this.listSize = listSize;
    this.startPath = startPath;
    this.fileStatusListProvider = fileStatusListProvider;
    List<OzoneFileStatusLight> nextListOfFiles = getNextListOfFiles();
    this.currentIterator = nextListOfFiles.iterator();
    this.singleDirectoryIterator = nextListOfFiles.size() == 1 && nextListOfFiles.get(0).isDirectory();
  }

  @Override
  public boolean hasNext() {
    if (!currentIterator.hasNext() && currentFileStatus != null) {
      if (singleDirectoryIterator || currentFileStatus.isDirectory()) {
        return false;
      }
      startPath = currentFileStatus.getKeyInfo().getKeyName();
      List<OzoneFileStatusLight> nextListOfFiles = getNextListOfFiles();
      currentIterator = nextListOfFiles.iterator();
      if (!nextListOfFiles.isEmpty()) {
        currentIterator.next();
      }
    }
    return currentIterator.hasNext();
  }

  @Override
  public OzoneFileStatusLight next() {
    if (hasNext()) {
      currentFileStatus = currentIterator.next();
      return currentFileStatus;
    }
    throw new NoSuchElementException();
  }

  private List<OzoneFileStatusLight> getNextListOfFiles() {
    List<OzoneFileStatusLight> list = fileStatusListProvider.getList(omKeyArgs, listSize, startPath);
    return list;
  }

  /**
   * Functional interface for providing list of file statuses.
   */
  @FunctionalInterface
  public interface FileStatusListProvider {
    List<OzoneFileStatusLight> getList(OmKeyArgs omKeyArgs, int listSize, String startPath);
  }

}
