package com.google.cloud.hadoop.gcsio.benchmarking;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * JMH benchmark for GoogleCloudStorageFileSystem.getItemInfo operation. Measures the performance of
 * metadata lookup for an existing GCS object.
 *
 * <p>To run this benchmark: 1. Ensure your 'gcsio' module is installed in your local Maven repo. 2.
 * Configure GCS credentials (e.g., GOOGLE_APPLICATION_CREDENTIALS env var). 3. Have a GCS bucket
 * ready (or the benchmark will fail at setup with a clear error). 4. From
 * 'hadoop-connectors-benchmarks' directory, run: mvn clean install java -jar target/benchmarks.jar
 * com.google.cloud.hadoop.gcsio.benchmarking.GCSGetItemInfoBenchmark
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3)
public class GCSGetItemInfoBenchmark {

  /** State class to set up and tear down GoogleCloudStorageFileSystem instance and test data. */
  @State(Scope.Benchmark)
  public static class BenchmarkState {
    // You can change this back to your previous bucket name if desired,
    // but keep a non-existent one for initial testing.
    @Param("yashjainn-test-bucket")
    public String bucketName;

    @Param("hello.cpp") // The name of the object to get info for
    public String objectName;

    private GoogleCloudStorage gcs;
    private GoogleCloudStorageFileSystem gcsFs;
    private URI testObjectUri; // URI for the object to query

    // Setup before all benchmark iterations (for a specific benchmark method and its params)
    @Setup(org.openjdk.jmh.annotations.Level.Trial)
    public void setup() throws IOException {
      System.out.println("Starting setup for bucket: " + bucketName + ", object: " + objectName);

      // 1. Configure GCS client options using the static builder() method
      GoogleCloudStorageOptions.Builder optionsBuilder = GoogleCloudStorageOptions.builder();
      optionsBuilder.setAppName("GCSGetItemInfoBenchmark");
      GoogleCloudStorageOptions gcsOptions = optionsBuilder.build();
      System.out.println("GCS Options built.");

      // Explicitly load credentials from the GOOGLE_APPLICATION_CREDENTIALS environment variable.
      GoogleCredentials credentials = null;
      try {
        credentials = GoogleCredentials.getApplicationDefault();
        System.out.println(
            "GoogleCredentials.getApplicationDefault() called. Credentials: "
                + (credentials != null ? "loaded" : "null"));
        if (credentials.createScopedRequired()) {
          credentials =
              credentials.createScoped(
                  Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
          System.out.println("Credentials scoped.");
        }
      } catch (Exception e) {
        System.err.println(
            "CRITICAL ERROR: Failed to load Google Credentials. Ensure GOOGLE_APPLICATION_CREDENTIALS is set correctly and points to a valid service account key JSON file.");
        System.err.println("Details: " + e.getMessage());
        e.printStackTrace();
        throw new IOException("Failed to load Google Credentials", e);
      }

      if (credentials == null) {
        throw new IOException("Google Credentials are null after loading attempt. Cannot proceed.");
      }

      // 2. Initialize the GoogleCloudStorageImpl client
      try {
        gcs =
            GoogleCloudStorageImpl.builder()
                .setOptions(gcsOptions)
                .setCredentials(credentials)
                .build();
        System.out.println("GoogleCloudStorageImpl client initialized.");
      } catch (Exception e) {
        System.err.println("CRITICAL ERROR: Failed to initialize GoogleCloudStorageImpl client.");
        System.err.println("Details: " + e.getMessage());
        e.printStackTrace();
        throw new IOException("Failed to initialize GCS client", e);
      }

      // 3. Initialize GoogleCloudStorageFileSystemImpl
      GoogleCloudStorageFileSystemOptions.Builder fsOptionsBuilder =
          GoogleCloudStorageFileSystemOptions.builder();
      GoogleCloudStorageFileSystemOptions fsOptions =
          fsOptionsBuilder.setCloudStorageOptions(gcsOptions).build();

      gcsFs = new GoogleCloudStorageFileSystemImpl(gcs, fsOptions);
      System.out.println("GoogleCloudStorageFileSystemImpl initialized.");

      // 4. VERIFY BUCKET EXISTENCE AND CREATE TEST OBJECT
      testObjectUri = URI.create("gs://" + bucketName + "/" + objectName);
      StorageResourceId bucketResourceId = new StorageResourceId(bucketName);
      StorageResourceId objectResourceId =
          new StorageResourceId(bucketName, objectName); // New: for the object

      try {
        // Attempt to get info for the bucket itself. This will throw an IOException
        // if the bucket does not exist. We'll inspect the cause.
        GoogleCloudStorageItemInfo bucketInfo = gcs.getItemInfo(bucketResourceId);

        if (!bucketInfo.exists()) {
          // This path should ideally not be reached if getItemInfo throws for non-existent buckets,
          // but included for robustness if it somehow returns a non-existent info object.
          System.err.println(
              "ERROR: Bucket '"
                  + bucketName
                  + "' explicitly reported as not existing by getItemInfo.");
          throw new IOException(
              "Bucket '"
                  + bucketName
                  + "' does not exist. Please create it manually before running the benchmark.");
        } else {
          System.out.println(
              "Bucket '" + bucketName + "' confirmed to exist. Proceeding to create object.");
        }

        // Create the test object in GCS
        // This line should only be reached if the bucket is confirmed to exist.
        gcs.create(new StorageResourceId(bucketName, objectName))
            .write(ByteBuffer.wrap("Hello, JMH Benchmark!".getBytes(StandardCharsets.UTF_8)));
        System.out.println("SUCCESS: Created test object: " + testObjectUri);

        // --- NEW VERIFICATION STEP ---
        System.out.println("Verifying object existence immediately after creation attempt...");
        GoogleCloudStorageItemInfo createdObjectInfo = gcs.getItemInfo(objectResourceId);
        if (createdObjectInfo.exists()) {
          System.out.println(
              "VERIFICATION: Test object '" + objectName + "' IS visible to GCS client.");
          System.out.println("  Size: " + createdObjectInfo.getSize());
          //          System.out.println("  Generation: " + createdObjectInfo.getGeneration());
        } else {
          System.err.println(
              "VERIFICATION ERROR: Test object '"
                  + objectName
                  + "' IS NOT visible to GCS client after creation attempt.");
          throw new IOException("Failed to verify object existence after creation.");
        }
        // --- END NEW VERIFICATION STEP ---

      } catch (IOException e) {
        // Check if the cause is a BucketNotFoundException from the underlying GCS client
        // (This is a common pattern when an SDK wraps its specific exceptions in a general
        // IOException)
        boolean isBucketNotFound = false;
        Throwable cause = e.getCause();
        while (cause != null) {
          // Using string check as the exact class might vary or be from a different package
          if (cause.getClass().getName().contains("BucketNotFoundException")) {
            isBucketNotFound = true;
            break;
          }
          cause = cause.getCause();
        }

        if (isBucketNotFound) {
          System.err.println("CRITICAL ERROR: Bucket '" + bucketName + "' does not exist!");
          System.err.println(
              "To run this benchmark, you MUST first create the bucket manually in Google Cloud Storage.");
          System.err.println("Example (using gsutil): gsutil mb gs://" + bucketName);
          System.err.println("Details: " + e.getMessage());
          throw new IOException(
              "Benchmark setup failed: GCS bucket '" + bucketName + "' not found.", e);
        } else {
          System.err.println(
              "CRITICAL ERROR: IOException encountered during test object creation or bucket check for bucket '"
                  + bucketName
                  + "'.");
          System.err.println("Details: " + e.getMessage());
          e.printStackTrace();
          throw e; // Re-throw any other IOExceptions
        }
      } catch (Exception e) {
        System.err.println(
            "CRITICAL ERROR: An unexpected exception occurred during benchmark setup for bucket '"
                + bucketName
                + "'.");
        System.err.println("Details: " + e.getMessage());
        e.printStackTrace();
        throw new IOException("Unexpected error during benchmark setup.", e);
      }
    }

    // Teardown after all benchmark iterations
    @TearDown(org.openjdk.jmh.annotations.Level.Trial)
    public void teardown() throws IOException {
      System.out.println("Starting teardown...");
      // Delete the test object
      StorageResourceId resourceId = new StorageResourceId(bucketName, objectName);
      // Use GoogleCloudStorageItemInfo for getItemInfo result
      try {
        if (gcs != null && ((GoogleCloudStorageItemInfo) gcs.getItemInfo(resourceId)).exists()) {
          gcs.deleteObjects(Collections.singletonList(resourceId));
          System.out.println("Deleted test object: " + testObjectUri);
        }
      } catch (IOException e) { // Catch general IOException during teardown
        // Check if the cause is a BucketNotFoundException or object not found
        boolean isNotFound = false;
        Throwable cause = e.getCause();
        while (cause != null) {
          if (cause.getClass().getName().contains("NotFoundException")
              || cause.getMessage().contains("No such object")) {
            isNotFound = true;
            break;
          }
          cause = cause.getCause();
        }
        if (isNotFound) {
          System.out.println(
              "Bucket or object not found during teardown; object likely already gone or never created. Details: "
                  + e.getMessage());
        } else {
          System.err.println("Error during test object deletion: " + e.getMessage());
          e.printStackTrace();
          throw e; // Re-throw if it's another type of error
        }
      } catch (Exception e) {
        System.err.println("Error during test object deletion (unexpected): " + e.getMessage());
        e.printStackTrace();
      }

      // Close the GCS FileSystem and Storage client
      if (gcsFs != null) {
        try {
          gcsFs.close();
          System.out.println("GoogleCloudStorageFileSystem closed.");
        } catch (Exception e) {
          System.err.println("Error closing GCS FileSystem: " + e.getMessage());
          e.printStackTrace();
        }
      }
      if (gcs != null) {
        // Safely close the GCS client.
        try {
          gcs.close();
          System.out.println("GoogleCloudStorage client closed.");
        } catch (NullPointerException e) {
          System.err.println(
              "Warning: NullPointerException during GCS client close. This is likely a cleanup issue in the Google Cloud Storage library and does not affect benchmark results. Details: "
                  + e.getMessage());
        } catch (Exception e) {
          System.err.println("Error closing GCS client: " + e.getMessage());
          e.printStackTrace();
        }
      }
      System.out.println("Teardown complete.");
    }
  }

  /** The actual benchmark method for getItemInfo. */
  @Benchmark
  public void benchmarkGetItemInfo(BenchmarkState state, Blackhole bh) throws IOException {
    // Cast or assign to GoogleCloudStorageItemInfo
    GoogleCloudStorageItemInfo itemInfo =
        (GoogleCloudStorageItemInfo)
            state.gcs.getItemInfo(new StorageResourceId(state.bucketName, state.objectName));
    bh.consume(itemInfo); // Consume the result to prevent dead code elimination
  }

  /** Main method to run the benchmark from the command line. */
  public static void main(String[] args) throws RunnerException {
    Options opt =
        new OptionsBuilder()
            // Include this benchmark class
            .include(GCSGetItemInfoBenchmark.class.getSimpleName())
            // Other JMH options can be added here, e.g., .forks(1)
            .build();
    new Runner(opt).run();
  }
}
