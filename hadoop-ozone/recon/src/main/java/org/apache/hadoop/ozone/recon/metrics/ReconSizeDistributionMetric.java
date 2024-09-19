package org.apache.hadoop.ozone.recon.metrics;

import com.google.inject.Singleton;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.ozone.OzoneConsts;
import org.hadoop.ozone.recon.schema.tables.pojos.ContainerCountBySize;
import org.hadoop.ozone.recon.schema.tables.pojos.FileCountBySize;

import java.util.HashMap;
import java.util.Map;

/**
 * Recon Size Distribution Metrics.ReconServer.
 */
@Singleton
@Metrics(about = "Recon Size Distribution Metrics", context = OzoneConsts.OZONE)
public class ReconSizeDistributionMetric implements MetricsSource {

  private static final String SOURCE_NAME = ReconSizeDistributionMetric.class.getSimpleName();

  private final Map<Long, Long> containerSizeCountMap = new HashMap<>();

  private final Map<FileSizeCountKey, Long> fileSizeCountMap = new HashMap<>();

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

    fileSizeCountMap.forEach((fsc, count) -> {
      if (count > 0) {
        MetricsRecordBuilder builder = collector.addRecord(SOURCE_NAME);

        builder.add(
            new MetricsTag(
                Interns.info("volume", "Volume"),
                fsc.volume));
        builder.add(
            new MetricsTag(
                Interns.info("bucket", "Bucket"),
                fsc.bucket));
        String inStorageUnits = getInStorageUnits((double) fsc.fileSizeUpperBound);
        builder.addCounter(Interns.info("fileSizeDistributionCount" + inStorageUnits,
                        inStorageUnits + "container size"), count);
        builder.endRecord();
      }
    });
  }

  public void fileSizeDistribution(FileCountBySize record) {
    FileSizeCountKey key = new FileSizeCountKey(record.getVolume(), record.getBucket(), record.getFileSize());

    fileSizeCountMap.put(key, record.getCount());
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

  private static class FileSizeCountKey {
    private final String volume;
    private final String bucket;
    private final Long fileSizeUpperBound;

    FileSizeCountKey(String volume, String bucket, Long fileSizeUpperBound) {
      this.volume = volume;
      this.bucket = bucket;
      this.fileSizeUpperBound = fileSizeUpperBound;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof FileSizeCountKey) {
        FileSizeCountKey s = (FileSizeCountKey) obj;
        return volume.equals(s.volume) && bucket.equals(s.bucket) && fileSizeUpperBound.equals(s.fileSizeUpperBound);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return (volume + bucket + fileSizeUpperBound).hashCode();
    }
  }
}
