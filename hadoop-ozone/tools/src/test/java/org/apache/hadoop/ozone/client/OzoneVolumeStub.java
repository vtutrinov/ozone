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

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Ozone volume with in-memory state for testing.
 */
public final class OzoneVolumeStub extends OzoneVolume {
  private static final Logger LOG = LoggerFactory.getLogger(OzoneVolumeStub.class);

  private final ClientProtocol proxy;

  public static Builder newBuilder(ClientProtocol proxy) {
    return new Builder(proxy);
  }

  private OzoneVolumeStub(Builder builder, ClientProtocol proxy) {
    super(builder);

    this.proxy = proxy;
  }

  @Override
  public void createBucket(String bucketName) throws IOException {
    proxy.createBucket(getName(), bucketName);
  }

  @Override
  public void createBucket(String bucketName, BucketArgs bucketArgs) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public OzoneBucket getBucket(String bucketName) throws IOException {
    return proxy.getBucketDetails(getName(), bucketName);
  }

  @Override
  public Iterator<? extends OzoneBucket> listBuckets(String bucketPrefix) {
    return listBuckets(bucketPrefix, null);
  }

  @Override
  public Iterator<? extends OzoneBucket> listBuckets(String bucketPrefix, String prevBucket) {
    try {
      return proxy.listBuckets(getName(), bucketPrefix, prevBucket, Integer.MAX_VALUE, true).iterator();
    } catch (IOException e) {
      LOG.error("Can't get list of buckets for volume {}", getName(), e);
      return Collections.emptyIterator();
    }
  }

  @Override
  public void deleteBucket(String bucketName) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public List<OzoneAcl> getAcls() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean addAcl(OzoneAcl addAcl) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean removeAcl(OzoneAcl acl) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean setAcl(List<OzoneAcl> acls) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  /**
   * Inner builder for OzoneVolumeStub.
   */
  public static final class Builder extends OzoneVolume.Builder {
    private final ClientProtocol proxy;

    private Builder(ClientProtocol proxy) {
      this.proxy = proxy;
    }

    @Override
    public Builder setName(String name) {
      super.setName(name);
      return this;
    }

    @Override
    public Builder setAdmin(String admin) {
      super.setAdmin(admin);
      return this;
    }

    @Override
    public Builder setOwner(String owner) {
      super.setOwner(owner);
      return this;
    }

    @Override
    public Builder setQuotaInBytes(long quotaInBytes) {
      super.setQuotaInBytes(quotaInBytes);
      return this;
    }

    @Override
    public Builder setQuotaInNamespace(long quotaInNamespace) {
      super.setQuotaInNamespace(quotaInNamespace);
      return this;
    }

    @Override
    public Builder setCreationTime(long creationTime) {
      super.setCreationTime(creationTime);
      return this;
    }

    @Override
    public Builder setAcls(List<OzoneAcl> acls) {
      super.setAcls(acls);
      return this;
    }

    @Override
    public OzoneVolumeStub build() {
      return new OzoneVolumeStub(this, proxy);
    }
  }
}
