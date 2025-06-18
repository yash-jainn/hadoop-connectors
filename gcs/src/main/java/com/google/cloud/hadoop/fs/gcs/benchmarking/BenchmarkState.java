// In a new file: com/google/cloud/hadoop/fs/gcs/benchmarking/BenchmarkState.java
package com.google.cloud.hadoop.fs.gcs.benchmarking;

import java.util.concurrent.atomic.AtomicBoolean;

public final class BenchmarkState {

  /**
   * The main on/off switch. Loaded ONCE from the initial Hadoop Configuration. Designed to be
   * flipped from true to false to ensure only the FIRST operation is benchmarked.
   */
  public static final AtomicBoolean IS_BENCHMARKING_ENABLED = new AtomicBoolean(false);

  /**
   * The "re-entrancy guard" flag. This is set to true only for the duration of a benchmark run to
   * prevent operations inside the benchmark from recursively triggering another benchmark. A
   * ThreadLocal is used for perfect thread safety.
   */
  public static final AtomicBoolean IS_IN_BENCHMARK_EXECUTION = new AtomicBoolean(false);
}
