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

package org.apache.hadoop.ozone.metric.util;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import java.util.StringJoiner;
import org.apache.hadoop.metrics2.MetricsRecord;

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
