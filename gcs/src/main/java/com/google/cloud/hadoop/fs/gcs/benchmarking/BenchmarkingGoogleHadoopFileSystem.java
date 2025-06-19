/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs.benchmarking;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import java.io.IOException;
import org.apache.hadoop.fs.Path;

/**
 * A wrapper around {@link GoogleHadoopFileSystem} that intercepts Hadoop FS commands and routes
 * them to the appropriate JMH benchmarks.
 *
 * <p>To use this, set the following property in the Hadoop configuration (e.g., core-site.xml):
 *
 * <pre>{@code
 * <property>
 * <name>fs.gs.impl</name>
 * <value>com.google.cloud.hadoop.fs.gcs.benchmarking.BenchmarkingGoogleHadoopFileSystem</value>
 * </property>
 * }</pre>
 *
 * <p>When this implementation is used, any file system operation (like rename, ls, create) will
 * trigger a JMH benchmark run instead of performing the actual operation.
 */
public class BenchmarkingGoogleHadoopFileSystem extends GoogleHadoopFileSystem {

  /** Intercepts the {@code rename} operation to trigger the {@link GCSRenameBenchmark}. */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    System.out.println("======================================================");
    System.out.println("  JMH BENCHMARK TRIGGERED FOR RENAME OPERATION!       ");
    System.out.println("  Source: " + src);
    System.out.println("  Destination: " + dst);
    System.out.println("======================================================");

    try {
      // Directly trigger the JMH benchmark for the rename operation.
      GCSRenameBenchmark.runBenchmark(src, dst);
    } catch (Exception e) {
      System.err.println("JMH benchmark failed to run: " + e.getMessage());
      throw new IOException("Failed to run JMH benchmark for rename", e);
    } finally {
      System.out.println("======================================================");
      System.out.println("  JMH BENCHMARK FINISHED FOR RENAME.                  ");
      System.out.println("======================================================");
    }

    // Return true to indicate success to the calling Hadoop command (e.g., 'hadoop fs -mv').
    return true;
  }

  /** Intercepts the {@code create} operation to trigger the {@link GCSCreateBenchmark}. */
  //    @Override
  //    public FSDataOutputStream create(
  //            Path f,
  //            FsPermission permission,
  //            boolean overwrite,
  //            int bufferSize,
  //            short replication,
  //            long blockSize,
  //            Progressable progress)
  //            throws IOException {
  //
  //        System.out.println("======================================================");
  //        System.out.println("  JMH BENCHMARK TRIGGERED FOR CREATE OPERATION!       ");
  //        System.out.println("  Path: " + f);
  //        System.out.println("======================================================");
  //
  //        try {
  //            // Directly trigger the JMH benchmark for the create operation.
  //            GCSCreateBenchmark.runBenchmark(f, overwrite);
  //        } catch (Exception e) {
  //            System.err.println("JMH benchmark failed to run: " + e.getMessage());
  //            throw new IOException("Failed to run JMH benchmark for create", e);
  //        } finally {
  //            System.out.println("======================================================");
  //            System.out.println("  JMH BENCHMARK FINISHED FOR CREATE.                  ");
  //            System.out.println("======================================================");
  //        }
  //
  //        // Return a valid, no-op stream to satisfy the Hadoop command framework.
  //        return new FSDataOutputStream(new IOUtils.NullOutputStream(), null);
  //    }
  //
  //    /**
  //     * Intercepts the {@code listStatus} operation to trigger the {@link
  // GCSListStatusBenchmark}.
  //     */
  //    @Override
  //    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
  //        System.out.println("======================================================");
  //        System.out.println("  JMH BENCHMARK TRIGGERED FOR LISTSTATUS OPERATION!   ");
  //        System.out.println("  Path: " + f);
  //        System.out.println("======================================================");
  //
  //        try {
  //            // Directly trigger the JMH benchmark for the listStatus operation.
  //            GCSListStatusBenchmark.runBenchmark(f);
  //        } catch (Exception e) {
  //            System.err.println("JMH benchmark failed to run: " + e.getMessage());
  //            throw new IOException("Failed to run JMH benchmark for listStatus", e);
  //        } finally {
  //            System.out.println("======================================================");
  //            System.out.println("  JMH BENCHMARK FINISHED FOR LISTSTATUS.              ");
  //            System.out.println("======================================================");
  //        }
  //
  //        // Return an empty array to prevent NullPointerExceptions in the caller.
  //        return new FileStatus[0];
  //    }

  /**
   * Intercepts the {@code copyFromLocalFile} operation to trigger the {@link
   * GCSCopyFromLocalBenchmark}.
   */
  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst)
      throws IOException {
    System.out.println("======================================================");
    System.out.println("  JMH BENCHMARK TRIGGERED FOR COPYFROMLOCAL!          ");
    System.out.println("  Source: " + src);
    System.out.println("  Destination: " + dst);
    System.out.println("======================================================");

    try {
      // Directly trigger the JMH benchmark for the copyFromLocal operation.
      GCSCopyFromLocalBenchmark.runBenchmark(src, dst);
    } catch (Exception e) {
      System.err.println("JMH benchmark failed to run: " + e.getMessage());
      throw new IOException("Failed to run JMH benchmark for copyFromLocalFile", e);
    } finally {
      System.out.println("======================================================");
      System.out.println("  JMH BENCHMARK FINISHED FOR COPYFROMLOCAL.           ");
      System.out.println("======================================================");
    }
  }
}
