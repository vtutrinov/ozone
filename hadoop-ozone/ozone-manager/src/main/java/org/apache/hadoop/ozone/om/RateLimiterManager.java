/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.RateLimiterInfo;
import org.apache.hadoop.ozone.om.ratelimiter.RateLimiter;
import org.apache.hadoop.ozone.om.ratelimiter.LeakyBucketRateLimiter;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RateLimiterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.om.ratelimiter.RateLimiterHelper.RATE_LIMITED_READ_CMDS;
import static org.apache.hadoop.ozone.om.ratelimiter.RateLimiterHelper.RATE_LIMITED_WRITE_CMDS;

/**
 * Manager class for rate limiters inside OzoneManager.
 * Loads limiter configuration from OM metadata, keeps in-memory instances,
 * and enforces limits for incoming requests.
 */
public class RateLimiterManager {

  private static final Logger LOG = LoggerFactory.getLogger(RateLimiterManager.class);

  private final ConcurrentMap<String, BucketLimiters> limitersByBucket =
          new ConcurrentHashMap<>();
  private final ConcurrentMap<OmRateLimiterMetrics.LimiterKey, PeriodState> periodStates =
          new ConcurrentHashMap<>();

  private final OMMetadataManager metadataManager;
  private final OmRateLimiterMetrics rateLimiterMetrics;

  public RateLimiterManager(OMMetadataManager metadataManager, OmRateLimiterMetrics rateLimiterMetrics)
      throws IOException {
    this.metadataManager = metadataManager;
    this.rateLimiterMetrics = rateLimiterMetrics;
    loadFromDb();
  }

  public void loadFromDb() throws IOException {
    Table<String, RateLimiterInfo> table =
              metadataManager.getRateLimiterInfoTable();

    int count = 0;
    try (TableIterator<String, ? extends Table.KeyValue<String, RateLimiterInfo>> iter =
                 table.iterator()) {
      while (iter.hasNext()) {
        Table.KeyValue<String, RateLimiterInfo> kv = iter.next();
        RateLimiterInfo info = kv.getValue();
        createOrUpdate(info);
        count++;
      }
    }
    LOG.info("Loaded {} rate limiters from OM DB into memory", count);
  }

  public void createOrUpdate(RateLimiterInfo info) {
    String bucketKey = metadataManager.getBucketKey(
            info.getVolumeName(), info.getBucketName());

    limitersByBucket.compute(bucketKey, (k, existing) -> {
      BucketLimiters bl = (existing != null) ? existing : new BucketLimiters();
      if (info.getType() == RateLimiterType.READ) {
        bl.setReadLimiter(new LeakyBucketRateLimiter(info.getRps()));
      } else {
        bl.setWriteLimiter(new LeakyBucketRateLimiter(info.getRps()));
      }
      return bl;
    });

    rateLimiterMetrics.updateCurrentQuota(
            info.getVolumeName(),
            info.getBucketName(),
            info.getType().name(),
            info.getRps());

    LOG.debug("Registered/updated rate limiter in memory: bucketKey={}, type={}, rps={}",
            bucketKey, info.getType(), info.getRps());
  }

  public void delete(String volume, String bucket, RateLimiterType type) {
    String bucketKey = metadataManager.getBucketKey(volume, bucket);

    limitersByBucket.computeIfPresent(bucketKey, (k, bl) -> {
      if (type == RateLimiterType.READ) {
        bl.setReadLimiter(null);
      } else {
        bl.setWriteLimiter(null);
      }
      if (bl.getReadLimiter() == null && bl.getWriteLimiter() == null) {
        return null;
      }
      return bl;
    });

    rateLimiterMetrics.removeRateLimiter(volume, bucket, type.name());
    LOG.debug("Removed rate limiter from memory: bucketKey={}, type={}",
            bucketKey, type);
  }

  public boolean tryAcquire(OzoneManagerProtocolProtos.OMRequest request, boolean isWrite) {
    if (limitersByBucket.isEmpty()) {
      return true;
    }
    
    OzoneManagerProtocolProtos.Type cmdType = request.getCmdType();

    if (isWrite) {
      if (!RATE_LIMITED_WRITE_CMDS.contains(cmdType)) {
        return true;
      }
    } else {
      if (!RATE_LIMITED_READ_CMDS.contains(cmdType)) {
        return true;
      }
    }

    VolumeBucketKey key = extractVolumeBucket(cmdType, request);
    if (key == null) {
      return true;
    }

    OzoneManagerProtocolProtos.RateLimiterType limiterType =
            isWrite ? OzoneManagerProtocolProtos.RateLimiterType.WRITE
                    : OzoneManagerProtocolProtos.RateLimiterType.READ;

    boolean allowed = tryAcquire(key.getVolume(), key.getBucket(), limiterType);
    if (!allowed) {
      LOG.warn("Rate limit exceeded: cmdType={}, volume={}, bucket={}, traceId={}",
              cmdType, key.getVolume(), key.getBucket(), request.getTraceID());
    }

    return allowed;
  }

  @VisibleForTesting
  public RateLimiter getLimiter(BucketLimiters limiters, RateLimiterType type) {
    return (type == RateLimiterType.READ) ? limiters.getReadLimiter() : limiters.getWriteLimiter();
  }

  public boolean tryAcquire(String volume, String bucket, RateLimiterType type) {
    String bucketKey = metadataManager.getBucketKey(volume, bucket);
    BucketLimiters limiters = limitersByBucket.get(bucketKey);
    if (limiters == null) {
      return true;
    }

    RateLimiter limiter = getLimiter(limiters, type);

    if (limiter == null) {
      return true;
    }

    boolean allowed = limiter.tryAcquire();

    long currentSecond = System.currentTimeMillis() / 1000;

    PeriodState state = getOrRollPeriod(volume, bucket, type.name(), currentSecond);
    int lastTotal;
    int lastRejected;
    double remainingRatio;

    synchronized (state) {
      state.currentTotal++;
      if (!allowed) {
        state.currentRejected++;
      }

      int quota = rateLimiterMetrics.getCurrentQuota(volume, bucket, type.name());
      int remaining = Math.max(quota - state.currentTotal, 0);
      remainingRatio = quota > 0 ? ((double)remaining) / quota : 0.0;

      lastTotal = state.lastTotal;
      lastRejected = state.lastRejected;
    }

    if (allowed) {
      rateLimiterMetrics.incAllowedRequests(volume, bucket, type.name());
    } else {
      rateLimiterMetrics.incRejectedRequests(volume, bucket, type.name());
    }

    rateLimiterMetrics.updatePeriodMetrics(volume, bucket, type.name(), lastTotal, lastRejected, remainingRatio);

    return allowed;
  }

  @VisibleForTesting
  PeriodState getOrRollPeriod(String volume, String bucket, String type, long currentSecond) {
    OmRateLimiterMetrics.LimiterKey key = new OmRateLimiterMetrics.LimiterKey(volume, bucket, type);
    PeriodState state = periodStates.computeIfAbsent(key, k -> new PeriodState(currentSecond));

    synchronized (state) {
      if (state.periodSecond != currentSecond) {
        state.lastTotal = state.currentTotal;
        state.lastRejected = state.currentRejected;

        state.currentTotal = 0;
        state.currentRejected = 0;

        state.periodSecond = currentSecond;
      }
    }
    return state;
  }

  @SuppressWarnings("checkstyle:methodlength")
  public VolumeBucketKey extractVolumeBucket(OzoneManagerProtocolProtos.Type cmdType,
                                             OzoneManagerProtocolProtos.OMRequest req) {
    switch (cmdType) {
    // ===== READ =====
    case LookupKey:
      OzoneManagerProtocolProtos.KeyArgs lkKeyArgs =
              req.getLookupKeyRequest().getKeyArgs();
      return new VolumeBucketKey(lkKeyArgs.getVolumeName(), lkKeyArgs.getBucketName());
    case ListKeys:
    case ListKeysLight:
      OzoneManagerProtocolProtos.ListKeysRequest lr =
              req.getListKeysRequest();
      return new VolumeBucketKey(lr.getVolumeName(), lr.getBucketName());
    case GetFileStatus:
      OzoneManagerProtocolProtos.GetFileStatusRequest gfs =
              req.getGetFileStatusRequest();
      return new VolumeBucketKey(gfs.getKeyArgs().getVolumeName(), gfs.getKeyArgs().getBucketName());
    case LookupFile:
      OzoneManagerProtocolProtos.LookupFileRequest lf =
              req.getLookupFileRequest();
      return new VolumeBucketKey(lf.getKeyArgs().getVolumeName(), lf.getKeyArgs().getBucketName());
    case ListStatus:
    case ListStatusLight:
      OzoneManagerProtocolProtos.ListStatusRequest ls =
              req.getListStatusRequest();
      return new VolumeBucketKey(ls.getKeyArgs().getVolumeName(), ls.getKeyArgs().getBucketName());
    case GetKeyInfo:
      OzoneManagerProtocolProtos.GetKeyInfoRequest gk =
              req.getGetKeyInfoRequest();
      OzoneManagerProtocolProtos.KeyArgs gkKeyArgs = gk.getKeyArgs();
      return new VolumeBucketKey(gkKeyArgs.getVolumeName(), gkKeyArgs.getBucketName());
    case InfoBucket:
      OzoneManagerProtocolProtos.InfoBucketRequest ib =
              req.getInfoBucketRequest();
      return new VolumeBucketKey(ib.getVolumeName(), ib.getBucketName());
    case ListTrash:
      OzoneManagerProtocolProtos.ListTrashRequest lt =
              req.getListTrashRequest();
      return new VolumeBucketKey(lt.getVolumeName(), lt.getBucketName());
    case ListMultiPartUploadParts:
      OzoneManagerProtocolProtos.MultipartUploadListPartsRequest mp =
              req.getListMultipartUploadPartsRequest();
      return new VolumeBucketKey(mp.getVolume(), mp.getBucket());
    case ListMultipartUploads:
      OzoneManagerProtocolProtos.ListMultipartUploadsRequest lm =
              req.getListMultipartUploadsRequest();
      return new VolumeBucketKey(lm.getVolume(), lm.getBucket());
    case ListSnapshot:
      OzoneManagerProtocolProtos.ListSnapshotRequest lsnap =
              req.getListSnapshotRequest();
      return new VolumeBucketKey(lsnap.getVolumeName(),
              lsnap.getBucketName());
    case SnapshotDiff:
      OzoneManagerProtocolProtos.SnapshotDiffRequest sd =
              req.getSnapshotDiffRequest();
      return new VolumeBucketKey(sd.getVolumeName(), sd.getBucketName());
    case CancelSnapshotDiff:
      OzoneManagerProtocolProtos.CancelSnapshotDiffRequest csd =
              req.getCancelSnapshotDiffRequest();
      return new VolumeBucketKey(csd.getVolumeName(), csd.getBucketName());
    case ListSnapshotDiffJobs:
      OzoneManagerProtocolProtos.ListSnapshotDiffJobRequest lj =
              req.getListSnapshotDiffJobRequest();
      return new VolumeBucketKey(lj.getVolumeName(), lj.getBucketName());
    case GetSnapshotInfo:
      OzoneManagerProtocolProtos.SnapshotInfoRequest si =
              req.getSnapshotInfoRequest();
      return new VolumeBucketKey(si.getVolumeName(), si.getBucketName());
    case GetContentSummary:
      OzoneManagerProtocolProtos.GetContentSummaryRequest csm =
              req.getGetContentSummaryRequest();
      return new VolumeBucketKey(csm.getKeyArgs().getVolumeName(), csm.getKeyArgs().getBucketName());
    case CreateKey:
      OzoneManagerProtocolProtos.CreateKeyRequest ck =
              req.getCreateKeyRequest();
      OzoneManagerProtocolProtos.KeyArgs ckKeyArgs = ck.getKeyArgs();
      return new VolumeBucketKey(ckKeyArgs.getVolumeName(), ckKeyArgs.getBucketName());
    case DeleteKey:
      OzoneManagerProtocolProtos.DeleteKeyRequest dk =
              req.getDeleteKeyRequest();
      OzoneManagerProtocolProtos.KeyArgs dkKeyArgs = dk.getKeyArgs();
      return new VolumeBucketKey(dkKeyArgs.getVolumeName(), dkKeyArgs.getBucketName());
    case CreateFile:
      OzoneManagerProtocolProtos.CreateFileRequest cf =
              req.getCreateFileRequest();
      OzoneManagerProtocolProtos.KeyArgs cfKeyArgs = cf.getKeyArgs();
      return new VolumeBucketKey(cfKeyArgs.getVolumeName(), cfKeyArgs.getBucketName());
    case DeleteKeys:
      OzoneManagerProtocolProtos.DeleteKeysRequest dks =
              req.getDeleteKeysRequest();
      return new VolumeBucketKey(dks.getDeleteKeys().getVolumeName(), dks.getDeleteKeys().getBucketName());
    case CreateBucket:
      OzoneManagerProtocolProtos.CreateBucketRequest cb =
              req.getCreateBucketRequest();
      return new VolumeBucketKey(cb.getBucketInfo().getVolumeName(),
              cb.getBucketInfo().getBucketName());
    case DeleteBucket:
      OzoneManagerProtocolProtos.DeleteBucketRequest db =
              req.getDeleteBucketRequest();
      return new VolumeBucketKey(db.getVolumeName(), db.getBucketName());
    case SetBucketProperty:
      OzoneManagerProtocolProtos.SetBucketPropertyRequest sb =
              req.getSetBucketPropertyRequest();
      return new VolumeBucketKey(sb.getBucketArgs().getVolumeName(),
              sb.getBucketArgs().getBucketName());
    case PurgeKeys:
      OzoneManagerProtocolProtos.PurgeKeysRequest pkr =
              req.getPurgeKeysRequest();

      if (pkr.getDeletedKeysCount() == 0) {
        return null;
      }

      OzoneManagerProtocolProtos.DeletedKeys first =
              pkr.getDeletedKeys(0);

      String volume = first.getVolumeName();
      String bucket = first.getBucketName();

      for (OzoneManagerProtocolProtos.DeletedKeys deletedKeys
              : pkr.getDeletedKeysList()) {
        if (!deletedKeys.getVolumeName().equals(volume)
                || !deletedKeys.getBucketName().equals(bucket)) {
          LOG.debug("PurgeKeysRequest contains multiple buckets; " +
                  "skipping rate limiting for this request");
          return null;
        }
      }
      return new VolumeBucketKey(volume, bucket);
    case CreateSnapshot:
      OzoneManagerProtocolProtos.CreateSnapshotRequest cs =
              req.getCreateSnapshotRequest();
      return new VolumeBucketKey(cs.getVolumeName(), cs.getBucketName());
    case DeleteSnapshot:
      OzoneManagerProtocolProtos.DeleteSnapshotRequest ds =
              req.getDeleteSnapshotRequest();
      return new VolumeBucketKey(ds.getVolumeName(), ds.getBucketName());
    case RenameSnapshot:
      OzoneManagerProtocolProtos.RenameSnapshotRequest rs =
              req.getRenameSnapshotRequest();
      return new VolumeBucketKey(rs.getVolumeName(), rs.getBucketName());
    case RecoverLease:
      OzoneManagerProtocolProtos.RecoverLeaseRequest rl =
              req.getRecoverLeaseRequest();
      return new VolumeBucketKey(rl.getVolumeName(), rl.getBucketName());
    case CreateDirectory:
      OzoneManagerProtocolProtos.CreateDirectoryRequest cd =
              req.getCreateDirectoryRequest();
      return new VolumeBucketKey(cd.getKeyArgs().getVolumeName(), cd.getKeyArgs().getBucketName());
    case AllocateBlock:
      OzoneManagerProtocolProtos.AllocateBlockRequest ab =
              req.getAllocateBlockRequest();
      OzoneManagerProtocolProtos.KeyArgs abKeyArgs = ab.getKeyArgs();
      return new VolumeBucketKey(abKeyArgs.getVolumeName(), abKeyArgs.getBucketName());
    case CommitKey:
      OzoneManagerProtocolProtos.CommitKeyRequest cmk =
              req.getCommitKeyRequest();
      OzoneManagerProtocolProtos.KeyArgs cmkKeyArgs = cmk.getKeyArgs();
      return new VolumeBucketKey(cmkKeyArgs.getVolumeName(), cmkKeyArgs.getBucketName());
    case RenameKey:
      OzoneManagerProtocolProtos.RenameKeyRequest rk =
              req.getRenameKeyRequest();
      OzoneManagerProtocolProtos.KeyArgs rkKeyArgs = rk.getKeyArgs();
      return new VolumeBucketKey(rkKeyArgs.getVolumeName(), rkKeyArgs.getBucketName());
    case RenameKeys:
      OzoneManagerProtocolProtos.RenameKeysRequest rks =
              req.getRenameKeysRequest();
      return new VolumeBucketKey(rks.getRenameKeysArgs().getVolumeName(),
              rks.getRenameKeysArgs().getBucketName());
    case InitiateMultiPartUpload:
      OzoneManagerProtocolProtos.KeyArgs mpuKeyArgs = req.getInitiateMultiPartUploadRequest().getKeyArgs();
      return new VolumeBucketKey(mpuKeyArgs.getVolumeName(), mpuKeyArgs.getBucketName());
    case CommitMultiPartUpload:
    case AbortMultiPartUpload:
    case CompleteMultiPartUpload:
      OzoneManagerProtocolProtos.KeyArgs commitKeyArgs = req.getCommitMultiPartUploadRequest().getKeyArgs();
      return new VolumeBucketKey(commitKeyArgs.getVolumeName(), commitKeyArgs.getBucketName());
    case SetTimes:
      OzoneManagerProtocolProtos.SetTimesRequest st =
              req.getSetTimesRequest();
      OzoneManagerProtocolProtos.KeyArgs stKeyArgs = st.getKeyArgs();
      return new VolumeBucketKey(stKeyArgs.getVolumeName(), stKeyArgs.getBucketName());
    default:
      return null;
    }
  }

  static final class VolumeBucketKey {
    private final String volume;
    private final String bucket;

    VolumeBucketKey(String volume, String bucket) {
      this.volume = volume;
      this.bucket = bucket;
    }

    String getVolume() {
      return volume;
    }

    String getBucket() {
      return bucket;
    }
  }

  /**
   * Read or write limiters for a bucket.
   */
  @VisibleForTesting
  public static final class BucketLimiters {
    private volatile RateLimiter readLimiter;
    private volatile RateLimiter writeLimiter;

    RateLimiter getReadLimiter() {
      return readLimiter;
    }

    void setReadLimiter(RateLimiter limiter) {
      this.readLimiter = limiter;
    }

    RateLimiter getWriteLimiter() {
      return writeLimiter;
    }

    void setWriteLimiter(RateLimiter limiter) {
      this.writeLimiter = limiter;
    }
  }

  static class PeriodState {
    private long periodSecond;

    private int currentTotal;
    private int currentRejected;

    private int lastTotal;
    private int lastRejected;

    PeriodState(long second) {
      this.periodSecond = second;
    }

    void setCurrentTotal(int currentTotal) {
      this.currentTotal = currentTotal;
    }

    void setCurrentRejected(int currentRejected) {
      this.currentRejected = currentRejected;
    }

    long getPeriodSecond() {
      return periodSecond;
    }

    int getCurrentTotal() {
      return currentTotal;
    }

    int getCurrentRejected() {
      return currentRejected;
    }

    int getLastTotal() {
      return lastTotal;
    }

    int getLastRejected() {
      return lastRejected;
    }
  }
}
