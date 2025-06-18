package com.google.cloud.hadoop.fs.gcs.benchmarking;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.openjdk.jmh.annotations.*;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class GCSListStatusBenchmark {

  private FileSystem fs;
  private Path bucketPath;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    // This configuration will be loaded from your Hadoop config files (e.g., core-site.xml)
    Configuration conf = new Configuration();

    // The bucket you want to benchmark.
    String bucket = "gs://yashjainn-test-bucket/";
    bucketPath = new Path(bucket);

    fs = FileSystem.get(URI.create(bucket), conf);
  }

  @TearDown(Level.Trial)
  public void teardown() throws IOException {
    fs.close();
  }

  @Benchmark
  public void benchmarkListStatus() throws IOException {
    // The operation to benchmark
    fs.listStatus(bucketPath);
  }
}
