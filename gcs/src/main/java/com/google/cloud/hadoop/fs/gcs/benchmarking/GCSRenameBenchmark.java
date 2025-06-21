package com.google.cloud.hadoop.fs.gcs.benchmarking;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import java.io.IOException;
import java.net.URI;
// Still needed for unique IDs in teardown cleanup of a general directory
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 0, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class GCSRenameBenchmark {

  // This field will hold the bucket name received from the Hadoop command.
  // JMH will populate this if -p is used directly, or it will be set via System Property.
  @Param({"NOT_SET"}) // Default value, should be overridden.
  public String bucketNameParam; // Renamed for clarity within the benchmark class.

  // New @Param fields for the actual source and destination paths
  @Param({"NOT_SET"})
  public String srcPathString;

  @Param({"NOT_SET"})
  public String dstPathString;

  // This instance will be used by the benchmark method for actual GCS operations.
  private GoogleHadoopFileSystem benchmarkGcsfs;
  private Path srcPath;
  private Path dstPath;

  // Base directory for temporary benchmark files inside the bucket
  // This might still be useful for other benchmarks if not for rename.
  private static final String BENCHMARK_TEMP_DIR = "/jmh-temp-rename-benchmarks/";

  @Setup(Level.Trial)
  public void setup() throws IOException {
    String currentBucketName = bucketNameParam;

    if (currentBucketName == null
        || currentBucketName.equals("NOT_SET")
        || currentBucketName.isEmpty()) {
      throw new IllegalArgumentException(
          "Benchmark bucket name must be provided via JMH @Param 'bucketNameParam'.");
    }

    if (srcPathString == null || srcPathString.equals("NOT_SET") || srcPathString.isEmpty()) {
      throw new IllegalArgumentException(
          "Source path must be provided via JMH @Param 'srcPathString'.");
    }

    if (dstPathString == null || dstPathString.equals("NOT_SET") || dstPathString.isEmpty()) {
      throw new IllegalArgumentException(
          "Destination path must be provided via JMH @Param 'dstPathString'.");
    }

    // Initialize a Configuration for the benchmark's FS instance
    Configuration benchmarkConfig = new Configuration();

    // Essential GCS-Connector configs
    benchmarkConfig.set("fs.gs.impl", GoogleHadoopFileSystem.class.getName());
    benchmarkConfig.set("fs.gs.implicit.directories.create", "false");
    benchmarkGcsfs = new GoogleHadoopFileSystem();
    benchmarkGcsfs.initialize(URI.create("gs://" + currentBucketName), benchmarkConfig);

    // Use the actual paths passed as parameters
    srcPath = new Path(srcPathString);
    dstPath = new Path(dstPathString);

    // Ensure the source file exists before each trial.
    // Overwrite if it already exists, as we need a clean slate for rename.
    //    try (FSDataOutputStream out = benchmarkGcsfs.create(srcPath, true)) {
    //      out.write("This is a dummy file for rename benchmark.".getBytes());
    //    }
    //    System.out.println("Setup: Created source file at: " + srcPath);

    // Delete destination file if it exists from previous runs to ensure clean rename.
    //    if (benchmarkGcsfs.exists(dstPath)) {
    //      benchmarkGcsfs.delete(dstPath, false);
    //      System.out.println("Setup: Deleted existing destination file at: " + dstPath);
    //    }
  }

  @TearDown(Level.Trial)
  public void teardown() throws IOException {
    // Clean up the created files after the benchmark trial
    if (benchmarkGcsfs != null) {
      benchmarkGcsfs.close();
      System.out.println("TearDown: Closed benchmark GCS FileSystem.");
    }
  }

  @Benchmark
  public boolean benchmarkRename() throws IOException {
    // This is the core logic that will be benchmarked
    // It calls the rename method on the 'benchmarkGcsfs' instance,
    // which has the benchmarking flag DISABLED, thus executing the original rename logic.
    return benchmarkGcsfs.rename(srcPath, dstPath);
  }

  // --- Static method called by GCS Connector's FileSystem implementation ---
  public static void runBenchmark(Path srcOriginalHadoopPath, Path dstOriginalHadoopPath)
      throws IOException {
    String bucketNameFromHadoopCommand = srcOriginalHadoopPath.toUri().getHost();

    System.out.println("======================================================");
    System.out.printf(" JMH BENCHMARK TRIGGERED FOR RENAME OPERATION!%n");
    System.out.printf(" Original Source Path (from Hadoop command): %s%n", srcOriginalHadoopPath);
    System.out.printf(
        " Original Destination Path (from Hadoop command): %s%n", dstOriginalHadoopPath);
    System.out.printf(
        " Running benchmark in bucket: %s (using actual paths)%n", bucketNameFromHadoopCommand);
    System.out.println("======================================================");

    try {
      Options opt =
          new OptionsBuilder()
              .include(GCSRenameBenchmark.class.getSimpleName())
              // Pass the actual source and destination paths as JMH parameters
              .param("bucketNameParam", bucketNameFromHadoopCommand)
              .param("srcPathString", srcOriginalHadoopPath.toString())
              .param("dstPathString", dstOriginalHadoopPath.toString())
              .build();

      new Runner(opt).run();

    } catch (RunnerException e) {
      throw new IOException("Failed to run JMH benchmark for rename operation", e);
    }
  }
}
