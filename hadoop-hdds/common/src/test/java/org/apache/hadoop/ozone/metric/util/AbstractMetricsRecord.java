package org.apache.hadoop.ozone.metric.util;

import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.thirdparty.com.google.common.base.Objects;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;

import java.util.StringJoiner;

abstract class AbstractMetricsRecord implements MetricsRecord {
  AbstractMetricsRecord() {
  }

  public boolean equals(Object obj) {
    if (!(obj instanceof MetricsRecord)) {
      return false;
    } else {
      MetricsRecord other = (MetricsRecord) obj;
      return Objects.equal(this.timestamp(), other.timestamp())
              && Objects.equal(this.name(), other.name())
              && Objects.equal(this.description(), other.description())
              && Objects.equal(this.tags(), other.tags())
              && Iterables.elementsEqual(this.metrics(), other.metrics());
    }
  }

  public int hashCode() {
    return Objects.hashCode(new Object[]{this.name(), this.description(), this.tags()});
  }

  public String toString() {
    return (new StringJoiner(", ", this.getClass().getSimpleName() + "{", "}"))
          .add("timestamp=" + this.timestamp())
          .add("name=" + this.name())
          .add("description=" + this.description())
          .add("tags=" + this.tags())
          .add("metrics=" + Iterables.toString(this.metrics()))
          .toString();
  }
}
