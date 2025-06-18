// In file: com/google/cloud/hadoop/fs/gcs/benchmarking/GCSCreateBenchmark.java
package com.google.cloud.hadoop.fs.gcs.benchmarking;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 1)
@Fork(1)
public class GCSCreateBenchmark {

  @Param({"_bucket_not_set_"})
  public String bucketNameParam;

  @Param({"1024"}) // 1 MB file size for the test write
  public int fileSizeKB;

  private GoogleHadoopFileSystem benchmarkGcsfs;
  private Path gcsDestinationFile;
  private byte[] dataToWrite;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    if ("_bucket_not_set_".equals(bucketNameParam)) {
      throw new IllegalArgumentException(
          "Bucket name must be provided via @Param 'bucketNameParam'");
    }

    Configuration benchmarkConfig = new Configuration();
    benchmarkConfig.set("fs.gs.impl", GoogleHadoopFileSystem.class.getName());
    benchmarkConfig.setBoolean(
        "fs.gs.benchmark.rename.enabled", false); // Use the correct configuration key
    // --- NEW CRITICAL ADDITION HERE ---
    // Explicitly set the static benchmark toggle to false for the JMH forked JVM.
    // This ensures it is false *before* the benchmark calls create(),
    // regardless of other Hadoop initializations that might have happened.
    BenchmarkState.IS_BENCHMARKING_ENABLED.set(false);
    // If 'benchmarkToggleInitialized' is accessible and static in GoogleHadoopFileSystem,
    // and you want to prevent *any* subsequent initializations in this JVM from re-enabling it,
    // you might also consider setting it to true:
    // GoogleHadoopFileSystem.setBenchmarkToggleInitialized(true); // (requires a setter or access
    // to the field)

    benchmarkGcsfs = new GoogleHadoopFileSystem();
    benchmarkGcsfs.initialize(URI.create("gs://" + bucketNameParam), benchmarkConfig);

    // Prepare a unique destination path for each trial run
    gcsDestinationFile =
        new Path(
            "gs://" + bucketNameParam + "/jmh-temp-create-benchmarks/file-" + UUID.randomUUID());

    // Prepare the data to write in memory
    long totalBytes = (long) fileSizeKB * 1024;
    dataToWrite = new byte[(int) totalBytes];
    // (Optionally fill the array with random data)
    System.out.println("Setup: Prepared to write " + fileSizeKB + " KB to " + gcsDestinationFile);
  }

  @TearDown(Level.Trial)
  public void teardown() throws IOException {
    // Clean up the file created by the benchmark
    if (benchmarkGcsfs != null) {
      benchmarkGcsfs.delete(gcsDestinationFile, false);
      benchmarkGcsfs.close();
      System.out.println("TearDown: Deleted file " + gcsDestinationFile);
    }
  }

  @Benchmark
  public void benchmarkCreateAndWrite() throws IOException {
    // Use try-with-resources to ensure the stream is ALWAYS closed.
    // The close() call is what finalizes the GCS upload.
    try (FSDataOutputStream out = benchmarkGcsfs.create(gcsDestinationFile, true)) {
      out.write(dataToWrite);
    }
  }
  // In file: GCSCreateBenchmark.java (add this static method)

  public static void runBenchmark(Path originalPath) throws IOException {
    // If this thread is already inside a benchmark run, exit immediately.
    //    if (BenchmarkState.IS_IN_BENCHMARK_EXECUTION.get()) {
    //      System.out.println("DEBUG: Recursive benchmark call detected for create. Skipping.");
    //      return;
    //    }
    //
    //    // Set the guard flag, run the benchmark, and guarantee cleanup.
    //    BenchmarkState.IS_IN_BENCHMARK_EXECUTION.set(true);
    try {
      String bucketName = originalPath.toUri().getHost();
      Options opt =
          new OptionsBuilder()
              .include(GCSCreateBenchmark.class.getSimpleName())
              .param("bucketNameParam", bucketName)
              // You can add more params here, e.g., for different file sizes
              // .param("fileSizeKB", "1024", "102400") // 1MB, 100MB
              .build();

      new Runner(opt).run();

    } catch (RunnerException e) {
      throw new IOException("Failed to run JMH benchmark for create", e);
    } finally {
      // CRITICAL: Always clear the guard flag to clean up the thread's state.
      //      BenchmarkState.IS_IN_BENCHMARK_EXECUTION.set(false);
    }
  }
}
