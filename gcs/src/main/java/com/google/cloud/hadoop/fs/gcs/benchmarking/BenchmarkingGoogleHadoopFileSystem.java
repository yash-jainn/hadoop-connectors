package com.google.cloud.hadoop.fs.gcs.benchmarking;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * A wrapper around {@link GoogleHadoopFileSystem} that intercepts Hadoop FS commands and routes
 * them to the appropriate JMH benchmarks.
 */
public class BenchmarkingGoogleHadoopFileSystem extends GoogleHadoopFileSystem {

  /** A functional interface for a benchmark action that can throw an Exception. */
  @FunctionalInterface
  private interface BenchmarkAction {
    void run() throws Exception;
  }

  /**
   * Generic helper to run a benchmark, printing start/end banners and handling exceptions.
   *
   * @param operationName The name of the operation being benchmarked (e.g., "RENAME").
   * @param action The lambda expression containing the benchmark logic to execute.
   * @throws IOException if the benchmark fails.
   */
  private void runBenchmarkAndLog(String operationName, BenchmarkAction action) throws IOException {
    String startMessage = String.format("JMH BENCHMARK TRIGGERED FOR %s OPERATION!", operationName);
    String endMessage = String.format("JMH BENCHMARK FINISHED FOR %s.", operationName);

    System.out.println("======================================================");
    System.out.println(String.format("  %-50s", startMessage));
    System.out.println("======================================================");

    try {
      action.run();
    } catch (Exception e) {
      System.err.println("JMH benchmark failed to run for " + operationName + ": " + e.getMessage());
      throw new IOException("Failed to run JMH benchmark for " + operationName, e);
    } finally {
      System.out.println("======================================================");
      System.out.println(String.format("  %-50s", endMessage));
      System.out.println("======================================================");
    }
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    runBenchmarkAndLog("RENAME", () -> GCSRenameBenchmark.runBenchmark(src, dst));
    System.out.println("\nBenchmark complete. Now performing the actual 'rename' operation...");
    return super.rename(src, dst);
  }

  @Override
  public FSDataOutputStream create(
          Path f,
          FsPermission permission,
          boolean overwrite,
          int bufferSize,
          short replication,
          long blockSize,
          Progressable progress)
          throws IOException {
    runBenchmarkAndLog(
            "CREATE",
            () ->
                    GCSCreateBenchmark.runBenchmark(
                            f, permission, overwrite, bufferSize, replication, blockSize, progress));
    System.out.println("\nBenchmark complete. Now performing the actual 'create' operation...");
    return super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst)
          throws IOException {
    runBenchmarkAndLog("COPYFROMLOCAL", () -> GCSCopyFromLocalFileBenchmark.runBenchmark(src, dst));
    System.out.println(
            "\nBenchmark complete. Now performing the actual 'copyFromLocalFile' operation...");
    super.copyFromLocalFile(delSrc, overwrite, src, dst);
  }

  @Override
  public FileStatus[] listStatus(Path hadoopPath) throws IOException {
    runBenchmarkAndLog("LISTSTATUS", () -> GCSListStatusBenchmark.runBenchmark(hadoopPath));
    System.out.println("\nBenchmark complete. Now performing the actual 'listStatus' operation...");
    return super.listStatus(hadoopPath);
  }

  @Override
  public FSDataInputStream open(Path hadoopPath, int bufferSize) throws IOException {
    runBenchmarkAndLog("OPEN", () -> GCSOpenBenchmark.runBenchmark(hadoopPath, bufferSize));
    System.out.println("\nBenchmark complete. Now performing the actual 'open' operation...");
    return super.open(hadoopPath, bufferSize);
  }
}
