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
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.util.StringUtils;

/**
 * Class to describe ozone filesystem content summary, it used on computing count and du of a directory.
 * Copied with minor changes from the hadoop project.
 */
public class ContentSummary extends QuotaUsage {

  private long length;
  private long fileCount;
  private long directoryCount;
  private long snapshotLength;
  private long snapshotFileCount;
  private long snapshotDirectoryCount;
  private long snapshotSpaceConsumed;
  private String erasureCodingPolicy;
  private static final String SUMMARY_FORMAT = "%12s %12s %18s ";
  private static final String[] SUMMARY_HEADER_FIELDS = new String[]{"DIR_COUNT", "FILE_COUNT", "CONTENT_SIZE"};
  private static final String SUMMARY_HEADER = String.format(
      SUMMARY_FORMAT, (Object[]) SUMMARY_HEADER_FIELDS);
  private static final String ALL_HEADER = QUOTA_HEADER + SUMMARY_HEADER;
  private static final String ERASURECODING_POLICY_FORMAT = "%20s ";
  private static final String ERASURECODING_POLICY_HEADER_FIELD = "ERASURECODING_POLICY";
  private static final String ERASURECODING_POLICY_HEADER = String.format(
      ERASURECODING_POLICY_FORMAT, ERASURECODING_POLICY_HEADER_FIELD);
  private static final String SNAPSHOT_FORMAT = "%18s %24s %24s %28s ";
  private static final String[] SNAPSHOT_HEADER_FIELDS =
      new String[] {"SNAPSHOT_LENGTH", "SNAPSHOT_FILE_COUNT",
          "SNAPSHOT_DIR_COUNT", "SNAPSHOT_SPACE_CONSUMED"};
  private static final String SNAPSHOT_HEADER =
      String.format(SNAPSHOT_FORMAT, (Object[]) SNAPSHOT_HEADER_FIELDS);

  public ContentSummary(Builder builder) {
    super(builder);
    this.length = builder.length;
    this.fileCount = builder.fileCount;
    this.directoryCount = builder.directoryCount;
    this.snapshotLength = builder.snapshotLength;
    this.snapshotFileCount = builder.snapshotFileCount;
    this.snapshotDirectoryCount = builder.snapshotDirectoryCount;
    this.snapshotSpaceConsumed = builder.snapshotSpaceConsumed;
    this.erasureCodingPolicy = builder.erasureCodingPolicy;
  }

  public Builder toBuilder() {
    return new Builder()
        .length(getLength())
        .fileCount(getFileCount())
        .directoryCount(getDirectoryCount())
        .snapshotLength(getSnapshotLength())
        .snapshotFileCount(getSnapshotFileCount())
        .snapshotDirectoryCount(getSnapshotDirectoryCount())
        .snapshotSpaceConsumed(getSnapshotSpaceConsumed())
        .quota(getQuota())
        .spaceQuota(getSpaceQuota())
        .spaceConsumed(getSpaceConsumed());
  }

  public ContentSummary combine(ContentSummary summary) {
    return this.toBuilder()
        .length(getLength() + summary.getLength())
        .fileCount(getFileCount() + summary.getFileCount())
        .directoryCount(getDirectoryCount() + summary.getDirectoryCount())
        .snapshotLength(getSnapshotLength() + summary.getSnapshotLength())
        .snapshotFileCount(getSnapshotFileCount() + summary.getSnapshotFileCount())
        .snapshotDirectoryCount(getSnapshotDirectoryCount() + summary.getSnapshotDirectoryCount())
        .snapshotSpaceConsumed(getSnapshotSpaceConsumed() + summary.getSnapshotSpaceConsumed())
        .quota(getQuota() + summary.getQuota())
        .spaceConsumed(getSpaceConsumed() + summary.getSpaceConsumed())
        .spaceQuota(getSpaceQuota() + summary.getSpaceQuota())
        .build();
  }

  public long getLength() {
    return length;
  }

  public long getFileCount() {
    return fileCount;
  }

  public long getDirectoryCount() {
    return directoryCount;
  }

  public long getSnapshotLength() {
    return snapshotLength;
  }

  public long getSnapshotFileCount() {
    return snapshotFileCount;
  }

  public long getSnapshotDirectoryCount() {
    return snapshotDirectoryCount;
  }

  public long getSnapshotSpaceConsumed() {
    return snapshotSpaceConsumed;
  }

  public String getErasureCodingPolicy() {
    return erasureCodingPolicy;
  }

  @Override
  public boolean equals(Object to) {
    if (this == to) {
      return true;
    } else if (to instanceof ContentSummary) {
      ContentSummary right = (ContentSummary) to;
      return getLength() == right.getLength() &&
          getFileCount() == right.getFileCount() &&
          getDirectoryCount() == right.getDirectoryCount() &&
          getSnapshotLength() == right.getSnapshotLength() &&
          getSnapshotFileCount() == right.getSnapshotFileCount() &&
          getSnapshotDirectoryCount() == right.getSnapshotDirectoryCount() &&
          getSnapshotSpaceConsumed() == right.getSnapshotSpaceConsumed() &&
          getErasureCodingPolicy().equals(right.getErasureCodingPolicy()) &&
          super.equals(to);
    } else {
      return super.equals(to);
    }
  }

  public org.apache.hadoop.fs.ContentSummary toHadoopContentSummary() {
    return new org.apache.hadoop.fs.ContentSummary.Builder()
        .length(getLength())
        .fileCount(getFileCount())
        .directoryCount(getDirectoryCount())
        .snapshotLength(getSnapshotLength())
        .snapshotFileCount(getSnapshotFileCount())
        .snapshotDirectoryCount(getSnapshotDirectoryCount())
        .snapshotSpaceConsumed(getSnapshotSpaceConsumed())
        .quota(getQuota())
        .spaceConsumed(getSpaceConsumed())
        .spaceQuota(getSpaceQuota())
        .build();
  }

  @Override
  public int hashCode() {
    long result = getLength() ^ getFileCount() ^ getDirectoryCount()
        ^ getSnapshotLength() ^ getSnapshotFileCount()
        ^ getSnapshotDirectoryCount() ^ getSnapshotSpaceConsumed()
        ^ getErasureCodingPolicy().hashCode();
    return ((int) result) ^ super.hashCode();
  }

  @Override
  public String toString() {
    return toString(true);
  }

  /** Return the string representation of the object in the output format.
   * if qOption is false, output directory count, file count, and content size;
   * if qOption is true, output quota and remaining quota as well.
   *
   * @param qOption a flag indicating if quota needs to be printed or not
   * @return the string representation of the object
   */
  @Override
  public String toString(boolean qOption) {
    return toString(qOption, false);
  }

  /** Return the string representation of the object in the output format.
   * For description of the options,
   * @see #toString(boolean, boolean, boolean, boolean, List)
   *
   * @param qOption a flag indicating if quota needs to be printed or not
   * @param hOption a flag indicating if human readable output if to be used
   * @return the string representation of the object
   */
  public String toString(boolean qOption, boolean hOption) {
    return toString(qOption, hOption, false, null);
  }

  /** Return the string representation of the object in the output format.
   * For description of the options,
   * @see #toString(boolean, boolean, boolean, boolean, List)
   *
   * @param qOption a flag indicating if quota needs to be printed or not
   * @param hOption a flag indicating if human readable output is to be used
   * @param xOption a flag indicating if calculation from snapshots is to be
   *                included in the output
   * @return the string representation of the object
   */
  public String toString(boolean qOption, boolean hOption, boolean xOption) {
    return toString(qOption, hOption, false, xOption, null);
  }

  /**
   * Return the string representation of the object in the output format.
   * For description of the options,
   * @see #toString(boolean, boolean, boolean, boolean, List)
   *
   * @param qOption a flag indicating if quota needs to be printed or not
   * @param hOption a flag indicating if human readable output if to be used
   * @param tOption a flag indicating if display quota by storage types
   * @param types Storage types to display
   * @return the string representation of the object
   */
  public String toString(boolean qOption, boolean hOption,
                         boolean tOption, List<StorageType> types) {
    return toString(qOption, hOption, tOption, false, types);
  }

  /** Return the string representation of the object in the output format.
   * if qOption is false, output directory count, file count, and content size;
   * if qOption is true, output quota and remaining quota as well.
   * if hOption is false, file sizes are returned in bytes
   * if hOption is true, file sizes are returned in human readable
   * if tOption is true, display the quota by storage types
   * if tOption is false, same logic with #toString(boolean,boolean)
   * if xOption is false, output includes the calculation from snapshots
   * if xOption is true, output excludes the calculation from snapshots
   *
   * @param qOption a flag indicating if quota needs to be printed or not
   * @param hOption a flag indicating if human readable output is to be used
   * @param tOption a flag indicating if display quota by storage types
   * @param xOption a flag indicating if calculation from snapshots is to be
   *                included in the output
   * @param types Storage types to display
   * @return the string representation of the object
   */
  public String toString(boolean qOption, boolean hOption, boolean tOption,
                         boolean xOption, List<StorageType> types) {
    String prefix = "";

    if (tOption) {
      return getTypesQuotaUsage(hOption, types);
    }

    if (qOption) {
      prefix = getQuotaUsage(hOption);
    }

    if (xOption) {
      return prefix + String.format(SUMMARY_FORMAT,
          formatSize(directoryCount - snapshotDirectoryCount, hOption),
          formatSize(fileCount - snapshotFileCount, hOption),
          formatSize(length - snapshotLength, hOption));
    } else {
      return prefix + String.format(SUMMARY_FORMAT,
          formatSize(directoryCount, hOption),
          formatSize(fileCount, hOption),
          formatSize(length, hOption));
    }
  }

  /**
   * Formats a size to be human readable or in bytes.
   * @param size value to be formatted
   * @param humanReadable flag indicating human readable or not
   * @return String representation of the size
   */
  private String formatSize(long size, boolean humanReadable) {
    return humanReadable
        ? StringUtils.TraditionalBinaryPrefix.long2String(size, "", 1)
        : String.valueOf(size);
  }

  /**
   * @return Constant-width String representation of Erasure Coding Policy
   */
  public String toErasureCodingPolicy() {
    return String.format(ERASURECODING_POLICY_FORMAT,
        erasureCodingPolicy.equals("Replicated")
            ? erasureCodingPolicy : "EC:" + erasureCodingPolicy);
  }

  /**
   * Return the string representation of the snapshot counts in the output
   * format.
   * @param hOption flag indicating human readable or not
   * @return String representation of the snapshot counts
   */
  public String toSnapshot(boolean hOption) {
    return String.format(SNAPSHOT_FORMAT, formatSize(snapshotLength, hOption),
        formatSize(snapshotFileCount, hOption),
        formatSize(snapshotDirectoryCount, hOption),
        formatSize(snapshotSpaceConsumed, hOption));
  }

  /**
   * Builder for ContentSummary.
   */
  public static class Builder extends QuotaUsage.Builder {
    private long length;
    private long fileCount;
    private long directoryCount;
    private long snapshotLength;
    private long snapshotFileCount;
    private long snapshotDirectoryCount;
    private long snapshotSpaceConsumed;
    private String erasureCodingPolicy;

    public Builder length(long dataLength) {
      this.length = dataLength;
      return this;
    }

    public Builder fileCount(long filesCount) {
      this.fileCount = filesCount;
      return this;
    }

    public Builder directoryCount(long directoriesCount) {
      this.directoryCount = directoriesCount;
      return this;
    }

    public Builder snapshotLength(long snapshotsLength) {
      this.snapshotLength = snapshotsLength;
      return this;
    }

    public Builder snapshotFileCount(long snapshotFilesCount) {
      this.snapshotFileCount = snapshotFilesCount;
      return this;
    }

    public Builder snapshotDirectoryCount(long snapshotDirectoriesCount) {
      this.snapshotDirectoryCount = snapshotDirectoriesCount;
      return this;
    }

    public Builder snapshotSpaceConsumed(long snapshotsSpaceConsumed) {
      this.snapshotSpaceConsumed = snapshotsSpaceConsumed;
      return this;
    }

    public Builder erasureCodingPolicy(String ecPolicy) {
      this.erasureCodingPolicy = ecPolicy;
      return this;
    }

    @Override
    public Builder quota(long quota) {
      super.quota(quota);
      return this;
    }

    @Override
    public Builder spaceConsumed(long spaceConsumed) {
      super.spaceConsumed(spaceConsumed);
      return this;
    }

    @Override
    public Builder spaceQuota(long spaceQuota) {
      super.spaceQuota(spaceQuota);
      return this;
    }

    public Builder incDirCount() {
      this.directoryCount++;
      return this;
    }

    @Override
    public ContentSummary build() {
      return new ContentSummary(this);
    }
  }

}
