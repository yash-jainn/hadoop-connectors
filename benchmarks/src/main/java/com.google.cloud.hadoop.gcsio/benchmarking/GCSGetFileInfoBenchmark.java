// GCSGetFileInfoBenchmark.java (Final version with Fork re-added)

package com.google.cloud.hadoop.gcsio.benchmarking;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3) // <--- RE-ADDED THIS LINE HERE
@State(Scope.Benchmark)
public class GCSGetFileInfoBenchmark {

  // --- Configuration Parameters ---
  @Param({
    "yashjainn-test-bucket" // Replace with your actual benchmark bucket
  })
  public String bucketName;

  @Param({"test/a.out"})
  public String objectName; // The object/file name to query info for

  // --- GCS Client Instances ---
  // We need both the low-level GCS client (for object creation/deletion)
  // and the FileSystem client (for the getFileInfo benchmark)
  private GoogleCloudStorage googleCloudStorage; // Used for setup/teardown object operations
  private GoogleCloudStorageFileSystem gcsFs; // Used for the benchmarked getFileInfo operation
  private URI targetUri; // URI object to be used in the benchmark
  private StorageResourceId testResourceId; // For creating/deleting test object

  // --- Setup Method ---
  @Setup(Level.Trial) // Runs once for the entire benchmark run
  public void setup() throws Exception {
    // --- 1. Configure GCS client options ---
    GoogleCloudStorageOptions gcsOptions =
        GoogleCloudStorageOptions.builder().setAppName("GCSGetFileInfoBenchmark").build();

    // --- 2. Load Google Cloud Authentication Credentials ---
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    if (credentials.createScopedRequired()) {
      credentials =
          credentials.createScoped(
              Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    }

    // --- 3. Initialize low-level GoogleCloudStorageImpl client ---
    this.googleCloudStorage =
        GoogleCloudStorageImpl.builder().setOptions(gcsOptions).setCredentials(credentials).build();

    // --- 4. Initialize GoogleCloudStorageFileSystemOptions ---
    GoogleCloudStorageFileSystemOptions fsOptions =
        GoogleCloudStorageFileSystemOptions.builder().setCloudStorageOptions(gcsOptions).build();

    // --- 5. Initialize GoogleCloudStorageFileSystemImpl client ---
    // This is the client that exposes getFileInfo(URI)
    this.gcsFs = new GoogleCloudStorageFileSystemImpl(googleCloudStorage, fsOptions);

    // --- 6. Prepare the URI object that will be used repeatedly in the benchmark ---
    this.targetUri = new URI("gs://" + bucketName + "/" + objectName);
    this.testResourceId = new StorageResourceId(bucketName, objectName);

    // --- 7. Create the test object if it's the 'existing' one ---
    if (!objectName.equals("non-existent-object-xyz")) {
      // This is the "existing" object case
      if (!googleCloudStorage.getItemInfo(testResourceId).exists()) {
        // File does not exist, so create it
        System.err.println("Test object '" + targetUri + "' not found. Creating it now...");
        try (WritableByteChannel writeChannel = googleCloudStorage.create(testResourceId)) {
          writeChannel.write(
              ByteBuffer.wrap("Hello, JMH Benchmark!".getBytes(StandardCharsets.UTF_8)));
        }
        System.err.println("Test object '" + targetUri + "' created.");
      } else {
        // File already exists, skip creation
        System.err.println("Test object '" + targetUri + "' already exists. Skipping creation.");
      }
    } else {
      // This is the "non-existent" object case
      if (googleCloudStorage.getItemInfo(testResourceId).exists()) {
        // File exists unexpectedly, so delete it to ensure it's non-existent
        System.err.println(
            "Non-existent test object '" + targetUri + "' found. Deleting it now...");
        googleCloudStorage.deleteObjects(Collections.singletonList(testResourceId));
        System.err.println("Non-existent test object '" + targetUri + "' deleted.");
      } else {
        // File correctly does not exist, no action needed
        System.err.println(
            "Non-existent test object '" + targetUri + "' is correctly not present.");
      }
    }

    System.err.println("Benchmark Setup complete. Target URI: '" + targetUri + "'");
  }

  // --- Teardown Method ---
  @TearDown(Level.Trial) // Runs once after all benchmark iterations are complete
  public void teardown() throws IOException {
    // Only delete the "existing" test object that was created or expected to exist
    if (!objectName.equals("non-existent-object-xyz")) {
      if (googleCloudStorage != null && googleCloudStorage.getItemInfo(testResourceId).exists()) {
        System.err.println("Deleting test object created for benchmark: " + targetUri);
        googleCloudStorage.deleteObjects(Collections.singletonList(testResourceId));
      }
    }

    // Close the GCS FileSystem and the underlying GCS client
    if (gcsFs != null) {
      gcsFs.close();
    }
    if (googleCloudStorage != null) {
      try {
        googleCloudStorage.close();
      } catch (NullPointerException e) {
        System.err.println(
            "Warning: NullPointerException during GCS client close. Details: " + e.getMessage());
      }
    }
    System.err.println("Benchmark Teardown complete.");
  }

  // --- Benchmark Method ---
  @Benchmark
  public void benchmarkGetFileInfo(Blackhole bh) throws Exception {
    FileInfo fileInfoResult = null;
    try {
      fileInfoResult = gcsFs.getFileInfo(targetUri);

      // --- NEW: Throw IOException if file does not exist ---
      if (fileInfoResult != null && !fileInfoResult.exists()) {
        // If FileInfo was returned but indicates non-existence, throw a custom IOException
        throw new IOException("Benchmark explicit failure: File does not exist at " + targetUri);
      }
      // --- END NEW ---

    } catch (IOException e) {
      // This catch block will now capture both actual I/O errors AND our custom thrown exception
      System.err.println("Caught exception for " + targetUri + ": " + e.getMessage());
      // IMPORTANT: Re-throw the exception so JMH records this iteration as a failure
      throw e;
    }

    bh.consume(fileInfoResult);
  }

  public static void main(String[] args) throws RunnerException {
    // Build the JMH options programmatically
    Options opt =
        new OptionsBuilder()
            // Include this benchmark class (make sure the class name is correct)
            .include(GCSGetFileInfoBenchmark.class.getSimpleName())

            // --- Crucial Debugging Options (often used to avoid forking in IDE) ---
            .forks(0) // Disables forking, your breakpoints will be hit in the main JVM.
            .warmupForks(0) // Also disable forks for the warmup phase.

            // --- Optional (but highly recommended) for quicker debugging ---
            .warmupIterations(1) // Run only 1 warmup iteration (or 0 to skip entirely).
            .measurementIterations(1) // Run only 1 measurement iteration.
            .mode(Mode.AverageTime) // Ensure this matches your benchmark's @BenchmarkMode.
            .timeUnit(
                TimeUnit.MILLISECONDS) // Ensure this matches your benchmark's @OutputTimeUnit.
            .shouldDoGC(true) // Helps ensure garbage collection runs between iterations.
            .jvmArgs(
                "-Djmh.executor=SAME_THREAD") // Forces execution on the same thread, simplifying
            // debugging.

            // --- Your Benchmark Parameters (if applicable) ---
            // If your benchmark uses @Param annotations, you can set them here.
            // Example:
            // .param("bucketName", "your-debug-bucket")
            // .param("objectName", "your-debug-object")

            .build();

    new Runner(opt).run();
  }
}
