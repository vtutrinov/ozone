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

package org.apache.hadoop.ozone.recon.metrics;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValueIterator;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.recon.spi.ReconFileMetadataManager;
import org.apache.hadoop.ozone.recon.tasks.FileSizeCountKey;
import org.apache.ozone.recon.schema.generated.tables.pojos.ContainerCountBySize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon size distribution metrics: container sizes and file sizes per volume/bucket.
 */
@Singleton
@Metrics(about = "Recon Size Distribution Metrics", context = OzoneConsts.OZONE)
public class ReconSizeDistributionMetric implements MetricsSource {

  private static final String SOURCE_NAME = ReconSizeDistributionMetric.class.getSimpleName();

  private static final Logger LOG = LoggerFactory.getLogger(ReconSizeDistributionMetric.class);

  private final Map<Long, Long> containerSizeCountMap = new ConcurrentHashMap<>();

  private final ReconFileMetadataManager reconFileMetadataManager;

  @Inject
  public ReconSizeDistributionMetric(ReconFileMetadataManager reconFileMetadataManager) {
    this.reconFileMetadataManager = reconFileMetadataManager;
  }

  public void register() {
    DefaultMetricsSystem.instance().register(SOURCE_NAME, "Recon Size Distribution Metrics", this);
  }

  public void unregister() {
    DefaultMetricsSystem.instance().unregisterSource(SOURCE_NAME);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    long total = containerSizeCountMap.values().stream().mapToLong(Long::longValue).sum();

    containerSizeCountMap.forEach((size, count) -> {
      if (count > 0) {
        MetricsRecordBuilder builder = collector.addRecord(SOURCE_NAME);

        String inStorageUnits = getInStorageUnits((double) size);
        builder.addGauge(Interns.info("containerDistributionPercent" + inStorageUnits,
                        inStorageUnits + "container size %"), Math.round(100.0 * count / total));
        builder.addCounter(Interns.info("containerDistributionCount" + inStorageUnits,
                        inStorageUnits + "container size"), count);
        builder.endRecord();
      }
    });

    // File size counts are kept in the Recon RocksDB by the FileSizeCountTask{FSO,OBS} tasks.
    try (KeyValueIterator<FileSizeCountKey, Long> iterator =
             reconFileMetadataManager.getFileCountTable().iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<FileSizeCountKey, Long> entry = iterator.next();
        FileSizeCountKey fsc = entry.getKey();
        Long count = entry.getValue();
        if (count != null && count > 0) {
          MetricsRecordBuilder builder = collector.addRecord(SOURCE_NAME);
          builder.add(new MetricsTag(Interns.info("volume", "Volume"), fsc.getVolume()));
          builder.add(new MetricsTag(Interns.info("bucket", "Bucket"), fsc.getBucket()));
          String inStorageUnits = getInStorageUnits((double) fsc.getFileSizeUpperBound());
          builder.addCounter(Interns.info("fileSizeDistributionCount" + inStorageUnits,
              inStorageUnits + "container size"), count);
          builder.endRecord();
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to read file size distribution from Recon DB", e);
    }
  }

  public void containerSizeDistribution(ContainerCountBySize record) {
    containerSizeCountMap.put(record.getContainerSize(), record.getCount());
  }

  private String getInStorageUnits(Double value) {
    double size;
    OzoneConsts.Units unit;
    if ((long) (value / OzoneConsts.TB) != 0) {
      size = value / OzoneConsts.TB;
      unit = OzoneConsts.Units.TB;
    } else if ((long) (value / OzoneConsts.GB) != 0) {
      size = value / OzoneConsts.GB;
      unit = OzoneConsts.Units.GB;
    } else if ((long) (value / OzoneConsts.MB) != 0) {
      size = value / OzoneConsts.MB;
      unit = OzoneConsts.Units.MB;
    } else if ((long) (value / OzoneConsts.KB) != 0) {
      size = value / OzoneConsts.KB;
      unit = OzoneConsts.Units.KB;
    } else {
      size = value;
      unit = OzoneConsts.Units.B;
    }
    return ((long) size) + " " + unit;
  }
}
