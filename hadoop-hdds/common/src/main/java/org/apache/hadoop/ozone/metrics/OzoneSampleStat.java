/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.metrics;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.util.SampleStat;

import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

/**
 * Helper to compute running sample stats.
 */
@InterfaceAudience.Private
public class OzoneSampleStat extends SampleStat {
  private final MinMax minmax = new MinMax();
  private LongAdder numSamples = new LongAdder();
  private DoubleAdder a0;
  private DoubleAdder a1 = new DoubleAdder();
  private DoubleAdder s0;
  private DoubleAdder s1 = new DoubleAdder();
  private DoubleAdder total;

  /**
   * Construct a new running sample stat.
   */
  public OzoneSampleStat() {
    a0 = new DoubleAdder();
    s0 = new DoubleAdder();
    total = new DoubleAdder();
  }

  public void reset() {
    numSamples.reset();
    a0.reset();
    s0.reset();
    total.reset();
    minmax.reset();
  }

  // We want to reuse the object, sometimes.
  void reset(long samplesCount, double a0Val, double a1Val, double s0Val,
             double s1Val, double totalVal, MinMax minmaxVal) {
    this.numSamples.reset();
    this.numSamples.add(samplesCount);

    this.a0.reset();
    this.a0.add(a0Val);

    this.a1.reset();
    this.a1.add(a1Val);

    this.s0.reset();
    this.s0.add(s0Val);

    this.s1.reset();
    this.s1.add(s1Val);

    this.total.reset();
    this.total.add(totalVal);

    this.minmax.reset(minmaxVal);
  }

  /**
   * Copy the values to other (saves object creation and gc.).
   * @param other the destination to hold our values
   */
  public void copyTo(OzoneSampleStat other) {
    other.reset(numSamples.sum(), a0.sum(), a1.sum(), s0.sum(), s1.sum(),
        total.sum(), minmax);
  }

  /**
   * Add a sample the running stat.
   * @param x the sample number
   * @return  self
   */
  public OzoneSampleStat add(double x) {
    minmax.add(x);
    return add(1, x);
  }

  /**
   * Add some sample and a partial sum to the running stat.
   * Note, min/max is not evaluated using this method.
   * @param nSamples  number of samples
   * @param x the partial sum
   * @return  self
   */
  public OzoneSampleStat add(long nSamples, double x) {
    numSamples.add(nSamples);
    total.add(x);

    if (numSamples.sum() == 1) {
      a0.reset();
      a0.add(x);

      a1.reset();
      a1.add(x);

      s0.reset();
      s0.add(0.0);
    } else {
      // The Welford method for numerical stability
      a1.reset();
      a1.add(a0.sum() + (x - a0.sum()) / numSamples.sum());

      s1.reset();
      s1.add(s0.sum() + (x - a0.sum()) * (x - a1.sum()));

      a0.reset();
      a0.add(a1.sum());
      s0.reset();
      s0.add(s1.sum());
    }
    return this;
  }

  /**
   * @return  the total number of samples
   */
  public long numSamples() {
    return numSamples.sum();
  }

  /**
   * @return the total of all samples added
   */
  public double total() {
    return total.sum();
  }

  /**
   * @return  the arithmetic mean of the samples
   */
  public double mean() {
    return numSamples.sum() > 0 ? (total.sum() / numSamples.sum()) : 0.0;
  }

  /**
   * @return  the variance of the samples
   */
  public double variance() {
    return numSamples.sum() > 1 ? s1.sum() / (numSamples.sum() - 1) : 0.0;
  }

  /**
   * @return  the standard deviation of the samples
   */
  public double stddev() {
    return Math.sqrt(variance());
  }

  /**
   * @return  the minimum value of the samples
   */
  public double min() {
    return minmax.min();
  }

  /**
   * @return  the maximum value of the samples
   */
  public double max() {
    return minmax.max();
  }

  @Override
  public String toString() {
    try {
      return "Samples = " + numSamples() +
          "  Min = " + min() +
          "  Mean = " + mean() +
          "  Std Dev = " + stddev() +
          "  Max = " + max();
    } catch (Throwable t) {
      return super.toString();
    }
  }

  /**
   * Helper to keep running min/max.
   */
  @SuppressWarnings("PublicInnerClass")
  public static class MinMax {

    // Float.MAX_VALUE is used rather than Double.MAX_VALUE, even though the
    // min and max variables are of type double.
    // Float.MAX_VALUE is big enough, and using Double.MAX_VALUE makes
    // Ganglia core due to buffer overflow.
    // The same reasoning applies to the MIN_VALUE counterparts.
    static final double DEFAULT_MIN_VALUE = Float.MAX_VALUE;
    static final double DEFAULT_MAX_VALUE = Float.MIN_VALUE;

    private AtomicDouble min = new AtomicDouble(DEFAULT_MIN_VALUE);
    private AtomicDouble max = new AtomicDouble(DEFAULT_MAX_VALUE);

    public void add(double value) {
      if (value > max.get()) {
        max.set(value);
      }
      if (value < min.get()) {
        min.set(value);
      }
    }

    public double min() {
      return min.get();
    }

    public double max() {
      return max.get();
    }

    public void reset() {
      min.set(DEFAULT_MIN_VALUE);
      max.set(DEFAULT_MAX_VALUE);
    }

    public void reset(MinMax other) {
      min.set(other.min());
      max.set(other.max());
    }
  }
}
