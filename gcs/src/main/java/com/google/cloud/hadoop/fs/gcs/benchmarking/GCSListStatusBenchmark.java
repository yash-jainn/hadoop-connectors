// In file: com/google/cloud/hadoop/fs/gcs/benchmarking/GCSListStatusBenchmark.java
package com.google.cloud.hadoop.fs.gcs.benchmarking;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 0, time = 1)
@Measurement(iterations = 1, time = 1)
@Fork(1)
public class GCSListStatusBenchmark {

  @Param({"_bucket_not_set_"})
  public String bucketNameParam;

  // Parameter to hold the real directory path to list.
  @Param({"_path_not_set_"})
  public String pathToListParam;

  private GoogleHadoopFileSystem benchmarkGcsfs;
  private Path directoryToList;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    if ("_bucket_not_set_".equals(bucketNameParam)) {
      throw new IllegalArgumentException(
          "Bucket name must be provided via @Param 'bucketNameParam'");
    }
    if ("_path_not_set_".equals(pathToListParam)) {
      throw new IllegalArgumentException(
          "Path to list must be provided via @Param 'pathToListParam'");
    }

    Configuration benchmarkConfig = new Configuration();
    benchmarkConfig.set("fs.gs.impl", GoogleHadoopFileSystem.class.getName());

    benchmarkGcsfs = new GoogleHadoopFileSystem();
    benchmarkGcsfs.initialize(URI.create("gs://" + bucketNameParam), benchmarkConfig);

    directoryToList = new Path(pathToListParam);

    System.out.println("Setup: Prepared to list contents of " + directoryToList);
  }

  @TearDown(Level.Trial)
  public void teardown() throws IOException {
    if (benchmarkGcsfs != null) {
      benchmarkGcsfs.close();
      System.out.println("TearDown: Closed benchmark filesystem instance.");
    }
  }

  @Benchmark
  public FileStatus[] benchmarkListStatus() throws IOException {
    // This is the core operation we want to measure, operating on the real directory.
    return benchmarkGcsfs.listStatus(directoryToList);
  }

  /**
   * Main entry point to run the benchmark from another class.
   *
   * @param pathToList The real GCS path of the directory to list.
   */
  public static void runBenchmark(Path pathToList) throws IOException {
    try {
      String bucketName = pathToList.toUri().getHost();
      Options opt =
          new OptionsBuilder()
              .include(GCSListStatusBenchmark.class.getSimpleName())
              .param("bucketNameParam", bucketName)
              // Pass the actual directory path to the benchmark instance.
              .param("pathToListParam", pathToList.toString())
              .build();

      new Runner(opt).run();

    } catch (RunnerException e) {
      throw new IOException("Failed to run JMH benchmark for listStatus", e);
    }
  }
}
