package com.google.cloud.hadoop.fs.gcs.benchmarking;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem.GCS_CONNECTOR_BENCHMARK_ENABLE; // Re-use this flag for now, or define a new one if you want separate control

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files; // For local file ops
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class GCSCopyFromLocalBenchmark { // Renamed for clarity (was GCSCopyFromLocalBenchmark)

  @Param({"_bucket_not_set_"})
  public String bucketNameParam;

  @Param({"1024"}) // Default file size in KB (1MB). Can be overridden via JMH param.
  public int fileSizeKB;

  private GoogleHadoopFileSystem benchmarkGcsfs;
  private FileSystem localFs; // To manage local temporary file
  private Path localSourceFilePath;
  private Path gcsDestinationFilePath;

  private static final String BENCHMARK_GCS_TEMP_DIR = "/jmh-temp-copy-benchmarks/";
  private static final String BENCHMARK_LOCAL_TEMP_DIR = "/tmp/jmh-temp-copy-benchmarks/";

  @Setup(Level.Trial)
  public void setup() throws IOException {
    String currentBucketName = bucketNameParam;

    if (currentBucketName == null
        || currentBucketName.equals("_bucket_not_set_")
        || currentBucketName.isEmpty()) {
      throw new IllegalArgumentException(
          "Benchmark bucket name must be provided via JMH @Param 'bucketNameParam'.");
    }
    if (fileSizeKB <= 0) {
      throw new IllegalArgumentException("File size for benchmark must be positive.");
    }

    Configuration benchmarkConfig = new Configuration();
    benchmarkConfig.setBoolean(GCS_CONNECTOR_BENCHMARK_ENABLE, false); // Disable recursive trigger
    benchmarkConfig.set("fs.gs.impl", GoogleHadoopFileSystem.class.getName());
    benchmarkConfig.set("fs.gs.implicit.directories.create", "false");

    // Initialize GCS FileSystem for the benchmark
    benchmarkGcsfs = new GoogleHadoopFileSystem();
    benchmarkGcsfs.initialize(URI.create("gs://" + currentBucketName), benchmarkConfig);

    // Initialize Local FileSystem for temporary file operations
    localFs = FileSystem.getLocal(new Configuration());

    // --- Create a temporary LOCAL source file ---
    String uniqueId = UUID.randomUUID().toString();
    java.nio.file.Path localTempDirPath = java.nio.file.Paths.get(BENCHMARK_LOCAL_TEMP_DIR);
    if (!Files.exists(localTempDirPath)) {
      Files.createDirectories(localTempDirPath);
    }
    localSourceFilePath =
        new Path(localTempDirPath.toString() + "/source-file-" + uniqueId + ".tmp");

    // Write dummy data to the local file
    try (OutputStream os =
        Files.newOutputStream(
            java.nio.file.Path.of(localSourceFilePath.toUri()),
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
      byte[] data = new byte[1024]; // 1 KB chunk
      long totalBytesToWrite = (long) fileSizeKB * 1024;
      for (long i = 0; i < totalBytesToWrite; i += data.length) {
        os.write(data, 0, (int) Math.min(data.length, totalBytesToWrite - i));
      }
    }
    System.out.println(
        "Setup: Created local source file of " + fileSizeKB + " KB at: " + localSourceFilePath);

    // --- Prepare the GCS destination path ---
    gcsDestinationFilePath =
        new Path(
            "gs://"
                + currentBucketName
                + BENCHMARK_GCS_TEMP_DIR
                + "destination-file-"
                + uniqueId
                + ".tmp");

    // Ensure destination directory exists on GCS if not using implicit directories
    // In GCS, directories are implicit, but it's good practice to ensure parent path if you use it
    // otherwise.
    // For simple file copies, this might not be strictly necessary as create() handles it.
  }

  @TearDown(Level.Trial)
  public void teardown() throws IOException {
    if (benchmarkGcsfs != null) {
      // Delete GCS destination file
      boolean deletedGcs = false;
      if (benchmarkGcsfs.exists(gcsDestinationFilePath)) {
        deletedGcs = benchmarkGcsfs.delete(gcsDestinationFilePath, false);
      }
      System.out.println(
          "TearDown: Deleted GCS destination file (" + deletedGcs + "): " + gcsDestinationFilePath);

      // Close GCS filesystem
      benchmarkGcsfs.close();
      System.out.println("TearDown: Closed benchmark GCS FileSystem.");
    }

    if (localFs != null) {
      // Delete local source file
      boolean deletedLocal = false;
      if (localFs.exists(localSourceFilePath)) {
        deletedLocal = localFs.delete(localSourceFilePath, false);
      }
      System.out.println(
          "TearDown: Deleted local source file (" + deletedLocal + "): " + localSourceFilePath);
      localFs.close(); // Close local FS
      System.out.println("TearDown: Closed local FileSystem.");
    }
  }

  @Benchmark
  public void benchmarkCopyFromLocal() throws IOException { // Change return type to void
    // This is the core logic that will be benchmarked
    // delSrc = false, overwrite = true (typical for benchmark to ensure clean state)
    benchmarkGcsfs.copyFromLocalFile( // No 'return' statement needed
        false, true, localSourceFilePath, gcsDestinationFilePath);
  }
  // --- Static method called by GCS Connector's FileSystem implementation ---
  public static void runBenchmark(Path srcOriginalHadoopPath, Path dstOriginalHadoopPath)
      throws IOException {

    //    // If this thread is already inside a benchmark run, exit immediately.
    //    // This is the re-entrancy guard that prevents recursion.
    //    if (BenchmarkState.IS_IN_BENCHMARK_EXECUTION.get()) {
    //      System.out.println("DEBUG: Recursive benchmark call detected. Skipping.");
    //      return;
    //    }
    //
    //    // Set the guard flag to true for this thread before starting.
    //    BenchmarkState.IS_IN_BENCHMARK_EXECUTION.set(true);

    // Your original entry message
    System.out.printf(" JMH BENCHMARK TRIGGERED FOR COPY FROM LOCAL OPERATION!%n");
    System.out.printf(" Original Source Path (from Hadoop command): %s%n", srcOriginalHadoopPath);
    System.out.printf(
        " Original Destination Path (from Hadoop command): %s%n", dstOriginalHadoopPath);
    String bucketNameFromHadoopCommand = dstOriginalHadoopPath.toUri().getHost();
    System.out.printf(
        " Running benchmark in bucket: %s (using temporary paths and controlled file size)%n",
        bucketNameFromHadoopCommand);

    try {
      Options opt =
          new OptionsBuilder()
              .include(GCSCopyFromLocalBenchmark.class.getSimpleName())
              .warmupIterations(1)
              .measurementIterations(1)
              .forks(1)
              .output("/tmp/jmh-copyFromLocal-out")
              .param("bucketNameParam", bucketNameFromHadoopCommand)
              .build();

      new Runner(opt).run();

    } catch (RunnerException e) {
      throw new IOException("Failed to run JMH benchmark for copy from local", e);
    } finally {
      // CRITICAL: ALWAYS clear the guard flag in a finally block.
      // This cleans up the state for this thread, allowing it to run other benchmarks later.
      //      BenchmarkState.IS_IN_BENCHMARK_EXECUTION.set(false);
    }
  }
}
