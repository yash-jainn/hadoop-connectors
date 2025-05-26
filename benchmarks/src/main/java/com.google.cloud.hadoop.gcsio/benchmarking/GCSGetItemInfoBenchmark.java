// GCSGetItemInfoBenchmark.java (Final version with appName set and GOOGLE_APPLICATION_CREDENTIALS
// explicit handling)

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
 * ready. 4. From 'hadoop-connectors-benchmarks' directory, run: mvn clean install java -jar
 * target/benchmarks.jar com.google.cloud.hadoop.gcsio.benchmarking.GCSGetItemInfoBenchmark
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
    @Param("yashjainn-test-bucket")
    public String bucketName;

    @Param("test-object-for-getiteminfo") // The name of the object to get info for
    public String objectName;

    private GoogleCloudStorage gcs;
    private GoogleCloudStorageFileSystem gcsFs;
    private URI testObjectUri; // URI for the object to query

    // Setup before all benchmark iterations (for a specific benchmark method and its params)
    @Setup(org.openjdk.jmh.annotations.Level.Trial)
    public void setup() throws IOException {
      // 1. Configure GCS client options using the static builder() method
      GoogleCloudStorageOptions.Builder optionsBuilder = GoogleCloudStorageOptions.builder();
      // ADD THIS LINE TO SET THE APPLICATION NAME
      optionsBuilder.setAppName("GCSGetItemInfoBenchmark");
      GoogleCloudStorageOptions gcsOptions = optionsBuilder.build();

      // Explicitly load credentials from the GOOGLE_APPLICATION_CREDENTIALS environment variable.
      GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
      if (credentials.createScopedRequired()) {
        credentials =
            credentials.createScoped(
                Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
      }

      // 2. Initialize the GoogleCloudStorageImpl client
      // Pass the explicitly loaded credentials to the builder
      gcs =
          GoogleCloudStorageImpl.builder()
              .setOptions(gcsOptions)
              .setCredentials(credentials)
              .build();

      // 3. Initialize GoogleCloudStorageFileSystemImpl
      GoogleCloudStorageFileSystemOptions.Builder fsOptionsBuilder =
          GoogleCloudStorageFileSystemOptions.builder();
      GoogleCloudStorageFileSystemOptions fsOptions =
          fsOptionsBuilder.setCloudStorageOptions(gcsOptions).build();

      gcsFs = new GoogleCloudStorageFileSystemImpl(gcs, fsOptions);

      // 4. Create the test object in GCS
      testObjectUri = URI.create("gs://" + bucketName + "/" + objectName);
      gcs.create(new StorageResourceId(bucketName, objectName))
          .write(ByteBuffer.wrap("Hello, JMH Benchmark!".getBytes(StandardCharsets.UTF_8)));
      System.out.println("Created test object: " + testObjectUri);
    }

    // Teardown after all benchmark iterations
    @TearDown(org.openjdk.jmh.annotations.Level.Trial)
    public void teardown() throws IOException {
      // Delete the test object
      StorageResourceId resourceId = new StorageResourceId(bucketName, objectName);
      // Use GoogleCloudStorageItemInfo for getItemInfo result
      if (gcs != null && ((GoogleCloudStorageItemInfo) gcs.getItemInfo(resourceId)).exists()) {
        gcs.deleteObjects(Collections.singletonList(resourceId));
        System.out.println("Deleted test object: " + testObjectUri);
      }

      // Close the GCS FileSystem and Storage client
      if (gcsFs != null) {
        gcsFs.close();
      }
      if (gcs != null) {
        // Safely close the GCS client.
        try {
          gcs.close();
        } catch (NullPointerException e) {
          System.err.println(
              "Warning: NullPointerException during GCS client close. This is likely a cleanup issue in the Google Cloud Storage library and does not affect benchmark results. Details: "
                  + e.getMessage());
        }
      }
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
