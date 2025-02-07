/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.hadoop.ozone.client;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * In-memory ozone bucket for testing.
 */
public final class OzoneBucketStub extends OzoneBucket {
  private final ClientProtocol proxy;

  private ReplicationConfig replicationConfig;

  public static Builder newBuilder(ClientProtocol proxy) {
    return new Builder(proxy);
  }

  private OzoneBucketStub(Builder builder, ClientProtocol proxy) {
    super(builder);
    this.proxy = proxy;
    this.replicationConfig = super.getReplicationConfig();
  }
  
  @Override
  public OzoneOutputStream createKey(String key, long size) throws IOException {
    return createKey(key, size, getReplicationConfig(), getMetadata());
  }

  @Override
  public OzoneOutputStream createKey(String key, long size, ReplicationConfig rConfig,
      Map<String, String> metadata) throws IOException {
    return proxy.createKey(
        getVolumeName(),
        getName(),
        key,
        size,
        rConfig,
        metadata
    );
  }

  @Override
  public OzoneDataStreamOutput createMultipartStreamKey(String key, long size, int partNumber, String uploadID) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public OzoneInputStream readKey(String key) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public OzoneKeyDetails getKey(String key) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public OzoneKey headObject(String key) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Iterator<? extends OzoneKey> listKeys(String keyPrefix) throws IOException {
    return listKeys(keyPrefix, null);
  }

  @Override
  public Iterator<? extends OzoneKey> listKeys(String keyPrefix, String prevKey) throws IOException {
    return listKeys(keyPrefix, prevKey, false);
  }

  @Override
  public Iterator<? extends OzoneKey> listKeys(String keyPrefix, String prevKey, boolean shallow) throws IOException {
    return proxy.listKeys(getVolumeName(), getName(), keyPrefix, prevKey, Integer.MAX_VALUE).iterator();
  }

  @Override
  public void deleteKey(String key) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void deleteKeys(List<String> keyList) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void renameKey(String fromKeyName, String toKeyName) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(String keyName, ReplicationConfig config) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public OzoneOutputStream createMultipartKey(String key, long size, int partNumber, String uploadID) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public OmMultipartUploadCompleteInfo completeMultipartUpload(String key, String uploadID,
                                                               Map<Integer, String> partsMap) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void abortMultipartUpload(String keyName, String uploadID) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public OzoneMultipartUploadPartListParts listParts(String key, String uploadID, int partNumberMarker, int maxParts) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public List<OzoneAcl> getAcls() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean removeAcl(OzoneAcl removeAcl) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean addAcl(OzoneAcl addAcl) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean setAcl(List<OzoneAcl> acls) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void setReplicationConfig(ReplicationConfig replicationConfig) {
    this.replicationConfig = replicationConfig;
  }

  @Override
  public ReplicationConfig getReplicationConfig() {
    return this.replicationConfig;
  }

  @Override
  public void createDirectory(String keyName) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  /**
   * Inner builder for OzoneBucketStub.
   */
  public static final class Builder extends OzoneBucket.Builder {
    private final ClientProtocol proxy;

    private Builder(ClientProtocol proxy) {
      this.proxy = proxy;
    }

    @Override
    public OzoneBucketStub build() {
      return new OzoneBucketStub(this, proxy);
    }
  }
}
