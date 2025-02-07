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

import java.util.List;
import java.util.Map;

/**
 * Interface for ozone volume. Describes methods that an ozone volume should implement.
 */
public interface Volume {

  String getName();

  String getAdmin();

  String getOwner();

  void setOwner(String owner);

  long getQuotaInBytes();

  void setQuotaInBytes(long quotaInBytes);

  long getQuotaInNamespace();

  void setQuotaInNamespace(long quotaInNamespace);

  long getUsedNamespace();

  void setUsedNamespace(long usedNamespace);

  long getCreationTime();

  void setCreationTime(long creationTime);

  long getModificationTime();

  void setModificationTime(long modificationTime);

  List<OzoneAcl> getAcls();

  boolean setAcls(List<OzoneAcl> acls);

  Map<String, String> getMetadata();

  void setMetadata(Map<String, String> metadata);

}
