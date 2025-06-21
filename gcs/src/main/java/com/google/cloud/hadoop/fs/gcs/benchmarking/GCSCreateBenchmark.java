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
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 2, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class GCSCreateBenchmark {

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    // These parameters are injected by the JMH runner from the OptionsBuilder
    @Param("")
    public String pathString;

    @Param("true")
    public boolean overwrite;

    @Param("131072")
    public int bufferSize;

    @Param("3")
    public short replication;

    @Param("134217728")
    public long blockSize;

    // These will be initialized in the setup method
    private GoogleHadoopFileSystem ghfs;
    private Path path;
    private FsPermission permission;
    private Progressable progress;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      this.path = new Path(pathString);
      this.permission = FsPermission.getFileDefault(); // Using standard default permissions
      this.progress = null; // Progress is typically null or a no-op for benchmarks

      Configuration conf = new Configuration();
      this.ghfs = new GoogleHadoopFileSystem();
      this.ghfs.initialize(this.path.toUri(), conf);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
      if (this.ghfs != null) {
        // Clean up the created file and close the filesystem
        this.ghfs.delete(this.path, false);
        this.ghfs.close();
      }
    }
  }

  @State(Scope.Thread)
  public static class InvocationState {
    FSDataOutputStream streamToClose;
  }

  @Benchmark
  public void create_InitiationOnly(BenchmarkState state, InvocationState invState, Blackhole bh)
      throws IOException {
    invState.streamToClose =
        state.ghfs.create(
            state.path,
            state.permission,
            state.overwrite,
            state.bufferSize,
            state.replication,
            state.blockSize,
            state.progress);
    bh.consume(invState.streamToClose);
  }

  @TearDown(Level.Invocation)
  public void tearDownInvocation(InvocationState invState) throws IOException {
    if (invState.streamToClose != null) {
      invState.streamToClose.close();
      invState.streamToClose = null;
    }
  }

  public static void runBenchmark(
      Path f,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException, RunnerException {

    // Build the JMH options, passing create() arguments as JMH parameters
    Options opt =
        new OptionsBuilder()
            .include(GCSCreateBenchmark.class.getSimpleName() + ".create_InitiationOnly")
            .param("pathString", f.toString())
            .param("overwrite", String.valueOf(overwrite))
            .param("bufferSize", String.valueOf(bufferSize))
            .param("replication", String.valueOf(replication))
            .param("blockSize", String.valueOf(blockSize))
            .build();

    new Runner(opt).run();
  }
}
