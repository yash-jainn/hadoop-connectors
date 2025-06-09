package com.google.cloud.hadoop.gcsio.benchmarking;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.gcsio.CreateObjectOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.ListObjectOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
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
@Fork(value = 0)
@State(Scope.Benchmark)
public class GCSRenameFolderBenchmark {

  // --- Benchmark Parameters ---
  @Param({"yashjainn-test-bucket"})
  public String bucketName;

  @Param({"my_source_folder/"})
  public String sourceFolderPrefix;

  @Param({"my_destination_folder/"})
  public String destinationFolderPrefix;

  @Param({"5"})
  public int numFilesPerFolder;

  // --- Internal Fields ---
  private GoogleCloudStorage googleCloudStorage;
  private GoogleCloudStorageFileSystemImpl gcsFs;

  private URI sourcePath;
  private URI destinationPath;

  private static final byte[] TEST_FILE_CONTENT =
      "This is a test file for the GCS rename benchmark.".getBytes(StandardCharsets.UTF_8);

  // --- Helper to list objects under a prefix ---
  private List<StorageResourceId> listAllObjectsUnderPrefix(String prefix) throws IOException {
    List<StorageResourceId> objects = new LinkedList<>();
    // Using the provided signature: List<GoogleCloudStorageItemInfo> listObjectInfo(bucketName,
    // objectNamePrefix, listOptions)
    List<GoogleCloudStorageItemInfo> listedItems =
        googleCloudStorage.listObjectInfo(bucketName, prefix, ListObjectOptions.DEFAULT);

    for (GoogleCloudStorageItemInfo item : listedItems) {
      // Check if it's an actual object (exists) and not a directory placeholder
      if (item.exists() && !item.isDirectory()) {
        objects.add(item.getResourceId());
      }
    }
    return objects;
  }

  // --- Setup Method (executed once per benchmark trial) ---
  @Setup(Level.Trial)
  public void setupTrial() throws Exception {
    System.out.println(
        "DEBUG (Trial Setup): Starting trial setup at " + System.currentTimeMillis());

    GoogleCloudStorageOptions gcsOptions =
        GoogleCloudStorageOptions.builder().setAppName("GCSRenameBenchmark").build();

    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    if (credentials.createScopedRequired()) {
      credentials =
          credentials.createScoped(
              Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    }

    this.googleCloudStorage =
        GoogleCloudStorageImpl.builder().setOptions(gcsOptions).setCredentials(credentials).build();

    GoogleCloudStorageFileSystemOptions fsOptions =
        GoogleCloudStorageFileSystemOptions.builder().setCloudStorageOptions(gcsOptions).build();

    this.gcsFs = new GoogleCloudStorageFileSystemImpl(this.googleCloudStorage, fsOptions);

    this.sourcePath = new URI("gs://" + bucketName + "/" + sourceFolderPrefix);
    this.destinationPath = new URI("gs://" + bucketName + "/" + destinationFolderPrefix);

    System.out.println(
        "DEBUG (Trial Setup): Benchmark trial setup complete. Source Folder: "
            + sourcePath
            + ", Destination Folder: "
            + destinationPath
            + ", Files per folder: "
            + numFilesPerFolder
            + " at "
            + System.currentTimeMillis());
  }

  // --- Setup Method (executed before EACH benchmark iteration) ---
  @Setup(Level.Iteration)
  public void setupIteration() throws IOException {
    System.out.println(
        "DEBUG (Iteration Setup): Starting setupIteration at " + System.currentTimeMillis());
    System.out.println("DEBUG (Iteration Setup): Target Source Folder: " + sourcePath);
    System.out.println("DEBUG (Iteration Setup): Target Destination Folder: " + destinationPath);

    // --- Robust Cleanup of FIXED folders before creating new files ---
    List<StorageResourceId> objectsToPreCleanup = new LinkedList<>();
    try {
      objectsToPreCleanup.addAll(listAllObjectsUnderPrefix(sourceFolderPrefix));
      objectsToPreCleanup.addAll(listAllObjectsUnderPrefix(destinationFolderPrefix));
    } catch (IOException e) {
      System.err.println(
          "ERROR (Iteration Setup): Failed to list objects for pre-cleanup: " + e.getMessage());
      e.printStackTrace();
      throw e; // Re-throw if listing fails, as we can't reliably clean up
    }

    if (!objectsToPreCleanup.isEmpty()) {
      System.out.println(
          "DEBUG (Iteration Setup): Found "
              + objectsToPreCleanup.size()
              + " existing objects for pre-cleanup. Deleting...");
      try {
        googleCloudStorage.deleteObjects(objectsToPreCleanup);
        System.out.println("DEBUG (Iteration Setup): Pre-cleanup successful.");
      } catch (IOException e) {
        System.err.println(
            "ERROR (Iteration Setup): Failed during pre-cleanup delete: " + e.getMessage());
        e.printStackTrace();
        throw e; // Critical failure if we can't clean up before creating
      }
    } else {
      System.out.println(
          "DEBUG (Iteration Setup): No existing objects found for pre-cleanup. Proceeding.");
    }

    // --- Create 'numFilesPerFolder' files inside the fixed source folder ---
    for (int i = 0; i < numFilesPerFolder; i++) {
      String fileName = "file_" + i + ".txt";
      StorageResourceId sourceFileResourceId =
          new StorageResourceId(bucketName, sourceFolderPrefix + fileName);

      try (WritableByteChannel channel =
          googleCloudStorage.create(
              sourceFileResourceId, CreateObjectOptions.DEFAULT_NO_OVERWRITE)) {
        channel.write(ByteBuffer.wrap(TEST_FILE_CONTENT));
      } catch (IOException e) {
        System.err.println(
            "ERROR (Iteration Setup): Failed to create source file "
                + sourceFileResourceId
                + ": "
                + e.getMessage());
        e.printStackTrace();
        throw e; // Re-throw to ensure JMH knows setup failed
      }
    }
    System.out.println(
        "DEBUG (Iteration Setup): Created " + numFilesPerFolder + " files in " + sourcePath);
    System.out.println(
        "DEBUG (Iteration Setup): setupIteration complete at " + System.currentTimeMillis());
  }

  // --- Benchmark Method ---
  @Benchmark
  public void benchmarkRename(Blackhole bh) throws IOException {
    long startTime = System.currentTimeMillis();
    // System.out.println("DEBUG (Benchmark): Starting benchmarkRename at " + startTime); //
    // Uncomment for verbose debug

    // Verify folder existence (check if any objects exist under the prefix)
    boolean sourceFolderExists = !listAllObjectsUnderPrefix(sourceFolderPrefix).isEmpty();
    System.out.println(
        "DEBUG (Benchmark): Source folder "
            + sourcePath
            + " exists BEFORE rename call: "
            + sourceFolderExists
            + " at "
            + System.currentTimeMillis());

    if (!sourceFolderExists) {
      System.err.println(
          "ERROR (Benchmark): Source folder "
              + sourcePath
              + " was NOT FOUND immediately before rename attempt!");
      // This indicates a severe issue in setup or a prior cleanup. Rename will likely fail.
    }

    try {
      // Perform the rename operation on the folder (prefix)
      gcsFs.rename(sourcePath, destinationPath);
      // System.out.println("DEBUG (Benchmark): Rename successful for folder " + sourcePath + " to "
      // + destinationPath + " in " + (System.currentTimeMillis() - startTime) + " ms."); //
      // Uncomment for verbose debug
    } catch (IOException e) {
      System.err.println(
          "ERROR (Benchmark): Rename failed for folder "
              + sourcePath
              + " to "
              + destinationPath
              + ": "
              + e.getMessage()
              + " after "
              + (System.currentTimeMillis() - startTime)
              + " ms.");
      e.printStackTrace(); // Print full stack trace for rename error
      throw e; // Re-throw to ensure JMH marks as a failure
    }
  }

  // --- Teardown Method (executed after EACH benchmark iteration) ---
  @TearDown(Level.Iteration)
  public void teardownIteration() {
    System.out.println(
        "DEBUG (Iteration Teardown): Starting teardownIteration at " + System.currentTimeMillis());

    List<StorageResourceId> objectsToCleanup = new LinkedList<>();

    try {
      // List objects under original source prefix (in case rename failed or partially succeeded)
      objectsToCleanup.addAll(listAllObjectsUnderPrefix(sourceFolderPrefix));
      // List objects under new destination prefix (after successful rename)
      objectsToCleanup.addAll(listAllObjectsUnderPrefix(destinationFolderPrefix));
    } catch (IOException e) {
      System.err.println(
          "ERROR (Iteration Teardown): Failed to list objects for cleanup: " + e.getMessage());
      e.printStackTrace();
    }

    if (!objectsToCleanup.isEmpty()) {
      System.out.println(
          "DEBUG (Iteration Teardown): Found "
              + objectsToCleanup.size()
              + " objects to clean up. Deleting...");
      try {
        googleCloudStorage.deleteObjects(objectsToCleanup);
        System.out.println("DEBUG (Iteration Teardown): Cleanup successful.");
      } catch (IOException e) {
        System.err.println(
            "ERROR (Iteration Teardown): Cleanup during teardown failed: " + e.getMessage());
        e.printStackTrace(); // Log cleanup failure but don't re-throw as iteration is ending anyway
      }
    } else {
      System.out.println("DEBUG (Iteration Teardown): No objects found for cleanup.");
    }

    System.out.println(
        "DEBUG (Iteration Teardown): teardownIteration complete at " + System.currentTimeMillis());
  }

  // --- Teardown Method (executed once after all benchmark trials) ---
  @TearDown(Level.Trial)
  public void teardownTrial() throws IOException {
    System.out.println(
        "DEBUG (Trial Teardown): Starting trial teardown at " + System.currentTimeMillis());
    if (gcsFs != null) {
      gcsFs.close();
      System.out.println("DEBUG (Trial Teardown): gcsFs closed.");
    } else if (googleCloudStorage != null) {
      googleCloudStorage.close();
      System.out.println("DEBUG (Trial Teardown): googleCloudStorage closed.");
    }
    System.out.println(
        "DEBUG (Trial Teardown): Benchmark trial teardown complete at "
            + System.currentTimeMillis());
  }

  // --- Main method to run the benchmark ---
  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
