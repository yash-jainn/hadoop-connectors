package com.google.cloud.hadoop.gcsio.benchmarking;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.gcsio.CreateObjectOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
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
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 50, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 50, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 0) // Using fork=0 to run in the same JVM as JMH harness
@State(Scope.Benchmark)
public class GCSRenameBenchmark {

  // --- Benchmark Parameters ---
  @Param({"yashjainn-test-bucket"}) // Your GCS bucket name
  public String bucketName;

  @Param({"renameSourceFile.txt"}) // Name of the source object to rename
  public String sourceObjectName;

  @Param({"renameDestinationFile.txt"}) // Name of the destination object after rename
  public String destinationObjectName;

  // --- Internal Fields ---
  private GoogleCloudStorage googleCloudStorage;
  private GoogleCloudStorageFileSystemImpl gcsFs;
  private StorageResourceId sourceResourceId;
  private StorageResourceId destinationResourceId;
  private URI sourcePath;
  private URI destinationPath;

  // Dummy content for the file we'll create
  private static final byte[] TEST_FILE_CONTENT =
      "This is a test file for the GCS rename benchmark.".getBytes(StandardCharsets.UTF_8);

  // --- Setup Method (executed once per benchmark trial) ---
  @Setup(Level.Trial)
  public void setupTrial() throws Exception {
    // 1. Configure GCS client options
    GoogleCloudStorageOptions gcsOptions =
        GoogleCloudStorageOptions.builder().setAppName("GCSRenameBenchmark").build();

    // 2. Load Google Cloud Authentication Credentials
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    if (credentials.createScopedRequired()) {
      credentials =
          credentials.createScoped(
              Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    }

    // 3. Initialize low-level GoogleCloudStorageImpl client
    this.googleCloudStorage =
        GoogleCloudStorageImpl.builder().setOptions(gcsOptions).setCredentials(credentials).build();

    // 4. Initialize GoogleCloudStorageFileSystemOptions
    GoogleCloudStorageFileSystemOptions fsOptions =
        GoogleCloudStorageFileSystemOptions.builder().setCloudStorageOptions(gcsOptions).build();

    // 5. Initialize GoogleCloudStorageFileSystemImpl client
    this.gcsFs = new GoogleCloudStorageFileSystemImpl(this.googleCloudStorage, fsOptions);

    // Initialize StorageResourceIds and URIs
    this.sourceResourceId = new StorageResourceId(bucketName, sourceObjectName);
    this.destinationResourceId = new StorageResourceId(bucketName, destinationObjectName);

    // Initialize URIs directly using the URI constructor
    this.sourcePath = new URI("gs://" + bucketName + "/" + sourceObjectName);
    this.destinationPath = new URI("gs://" + bucketName + "/" + destinationObjectName);
  }

  // --- Setup Method (executed before EACH benchmark iteration) ---
  @Setup(Level.Iteration)
  public void setupIteration() throws IOException {
    boolean destExistsBeforeCleanup = false;
    boolean sourceExistsBeforeCleanup = false;

    try {
      destExistsBeforeCleanup = googleCloudStorage.getItemInfo(destinationResourceId).exists();
      sourceExistsBeforeCleanup = googleCloudStorage.getItemInfo(sourceResourceId).exists();
    } catch (IOException e) {
      // In a real benchmark, you'd likely log this to a file or rethrow if it prevents setup.
      throw e; // Re-throw to fail the iteration if file existence check fails
    }

    if (destExistsBeforeCleanup) {
      try {
        googleCloudStorage.deleteObjects(ImmutableList.of(destinationResourceId));
      } catch (IOException e) {
        throw e; // Re-throw to fail the iteration if cleanup fails
      }
    }

    if (sourceExistsBeforeCleanup) {
      try {
        googleCloudStorage.deleteObjects(ImmutableList.of(sourceResourceId));
      } catch (IOException e) {
        throw e; // Re-throw to fail the iteration if cleanup fails
      }
    }

    try (WritableByteChannel channel =
        googleCloudStorage.create(sourceResourceId, CreateObjectOptions.DEFAULT_NO_OVERWRITE)) {
      channel.write(ByteBuffer.wrap(TEST_FILE_CONTENT));
    } catch (IOException e) {
      throw e; // Re-throw to fail the iteration if source file creation fails
    }
  }

  // --- Benchmark Method ---
  @Benchmark
  public void benchmarkRename(Blackhole bh) throws IOException {
    boolean sourceExistsBeforeRenameCall = gcsFs.exists(sourcePath);

    if (!sourceExistsBeforeRenameCall) {
      // For a benchmark, this indicates a critical setup failure.
      // JMH will mark this iteration as a failure.
      throw new IOException(
          "Source file " + sourcePath + " was NOT FOUND immediately before rename attempt!");
    }

    try {
      gcsFs.rename(sourcePath, destinationPath);
    } catch (IOException e) {
      // Rethrow the exception so JMH can track failed iterations
      throw e;
    }
    // Consume results to prevent dead code elimination by the JVM
    bh.consume(sourceExistsBeforeRenameCall);
  }

  // --- Teardown Method (executed after EACH benchmark iteration) ---
  @TearDown(Level.Iteration)
  public void teardownIteration() {
    try {
      // Attempt to clean up both source and destination in case rename failed or partially
      // completed
      googleCloudStorage.deleteObjects(ImmutableList.of(sourceResourceId, destinationResourceId));
    } catch (IOException e) {
      // Log cleanup errors or re-throw, depending on desired behavior for teardown.
      // For benchmarks, often just logging is sufficient, as teardown shouldn't fail the benchmark
      // itself.
      // However, if cleanup is critical for next iteration, you might rethrow.
      System.err.println(
          "ERROR (Iteration Teardown): Cleanup during teardown failed: " + e.getMessage());
    }
  }

  // --- Teardown Method (executed once after all benchmark trials) ---
  @TearDown(Level.Trial)
  public void teardownTrial() throws IOException {
    if (gcsFs != null) {
      gcsFs.close();
    } else if (googleCloudStorage != null) {
      googleCloudStorage.close();
    }
  }

  // --- Main method to run the benchmark ---
  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
