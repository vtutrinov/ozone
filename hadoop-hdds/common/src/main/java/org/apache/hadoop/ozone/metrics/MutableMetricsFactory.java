package org.apache.hadoop.ozone.metrics;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.lib.MutableMetric;

import java.lang.reflect.Field;

public class MutableMetricsFactory
    extends org.apache.hadoop.metrics2.lib.MutableMetricsFactory {

  @Override
  protected MutableMetric newForField(Field field, Metric annotation) {
    MetricsInfo info = getInfo(annotation, field);
    final Class<?> cls = field.getType();
    if (cls == MutableRate.class) {
      return new MutableRate(info.name(), info.description(),
          annotation.always());
    }
    return null;
  }
}
