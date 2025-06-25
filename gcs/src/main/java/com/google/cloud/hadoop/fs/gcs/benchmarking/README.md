# GCS Hadoop FileSystem Benchmarking: Common User Guide

### 1. Introduction & Goal

This framework provides a pluggable way to measure the real-world performance of specific Hadoop FileSystem operations (like ls, cat, put) on the GCS layer. It uses the robust JMH (Java Microbenchmark Harness) to ensure statistically significant results. Additionally, we will be using Profilers for better analysis of the underlying function.

The key feature is its ability to intercept standard `hadoop fs` commands, requiring no changes to user scripts—only a one-time configuration update.

### 2. How It Works: Architectural Overview

The system uses a Decorator/Wrapper pattern. A `GoogleHadoopFileSystemJMHBenchmarking` class "wraps" the real `GoogleHadoopFileSystem`.

When a user runs a command (e.g., `hadoop fs -ls`):

1.  Hadoop's configuration directs the call to our benchmarking wrapper.
2.  The wrapper launches the corresponding JMH benchmark in an isolated, timed environment.
3.  The benchmark calls the *real* GCS connector method multiple times to get a reliable measurement.
4.  After the benchmark completes, the wrapper executes the real command one final time so the user sees the expected output (e.g., the directory listing).

### 3. Prerequisites

* **GCS Connector JAR**: Copy the JAR file for the GCS connector to your VM. You will need this during the Hadoop installation steps.
* **Hadoop Installation**: Follow standard procedures to install Hadoop on your VM.
* **YourKit Profiler Installation**: Follow the standard installation steps for the YourKit profiler.

### 4. Getting Started: Common Setup

To activate the benchmarking framework for any command, you must modify your Hadoop `core-site.xml` configuration file to point to the wrapper implementation.

**Tip:** Set the path to `core-site.xml` and to the GCS JAR location as environment variables for convenience. Also, ensure you have the latest GCS JAR at the specified path.

```bash
export CORE_SITE=/path/to/your/hadoop/etc/hadoop/core-site.xml
export GCS_JAR_PATH=/path/to/your/hadoop/share/hadoop/common/lib
````

Now you can easily edit the configuration file:

```bash
sudo nano $CORE_SITE
```

**Add the following property to your `core-site.xml`:**

```xml
<property>
  <name>fs.gs.impl</name>
  <value>com.google.cloud.hadoop.fs.gcs.benchmarking.GoogleHadoopFileSystemJMHBenchmarking</value>
  <description>
    Overrides the default GCS filesystem to enable performance
    benchmarking on supported operations.
  </description>
</property>
```

Once this is configured, you are ready to run benchmarks for any supported command.

### 5\. Analyzing Benchmark Results

You get two forms of output from the benchmark:

* **Console Output**: A summary table from JMH will be printed directly in your terminal, showing the performance score (e.g., in milliseconds per operation), the confidence interval, and other relevant metrics.
* **JSON Report**: A file named `jmh-<operation>-results.json` can be generated (if enabled in the benchmark code). You can upload this JSON file to an online JMH visualizer to generate interactive HTML charts.

### 6\. Important Considerations

* **Performance Impact**: For accuracy, the benchmark runs the target operation multiple times. After it finishes, the wrapper runs it one last time to fulfill the original command. This means that running any command with benchmarking enabled will be **slower than normal**. This is expected and necessary for proper measurement.
* **Benchmark Isolation**: Each JMH benchmark runs in a separate forked JVM. This ensures that different benchmark runs do not interfere with each other, leading to more reliable and consistent results.

### 7\. Profiling with YourKit

To dive deeper into performance bottlenecks, you can use a profiler like YourKit. You can profile two distinct scenarios: the standard, end-to-end command and the isolated JMH benchmark execution.

*For more details on CPU profiling with YourKit, refer to the official YourKit User Guide.*

#### A. Profiling the Standard Operation (Baseline)

This measures the performance of the entire command without any benchmarking overhead.

1.  **Configure for Standard Operation**: In `core-site.xml`, ensure `fs.gs.impl` is set to the real filesystem:
    ```xml
    <property>
      <name>fs.gs.impl</name>
      <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem</value>
    </property>
    ```
2.  **Attach YourKit Agent**: When you run the `hadoop` command, attach the YourKit profiler agent to the JVM by setting the `HADOOP_OPTS` environment variable before running your command:
    ```bash
    export HADOOP_OPTS="-agentpath:<path_to_yourkit_agent>/bin/libyjpagent.so=tracing,onexit=snapshot,dir=/tmp/yourkit_snapshots"
    ```

#### B. Profiling the JMH Benchmark

This profiles the isolated benchmark run. Because JMH runs its tests in a **separate, forked JVM**, we need to ensure the profiler agent is attached to that forked process.

1.  **Configure for Benchmarking**: Ensure `fs.gs.impl` is set to your `GoogleHadoopFileSystemJMHBenchmarking` wrapper class in `core-site.xml`.

2.  **Attach YourKit Agent**: Use the same `HADOOP_OPTS` environment variable approach. The forked process will inherit these JVM options.

    ```bash
    export HADOOP_OPTS="-agentpath:<path_to_yourkit_agent>/bin/libyjpagent.so=tracing,onexit=snapshot,dir=/tmp/yourkit_snapshots"
    ```

3.  **Run and Capture**: Now, when you run the Hadoop command, the JMH fork will start with the profiler agent attached. You will get multiple snapshots (one for the main Hadoop FsShell process, and one for each JMH Benchmark called).

    **Note**: The main FsShell snapshot might be misleading in this case as it will include the time for all the JMH calls. To profile the underlying function calls made by the GCS connector, you should analyze the individual snapshots generated for each JMH benchmark fork.

### 8\. Sequence Diagram

*(A generic sequence diagram illustrating the interception and benchmarking flow would be included here.)*

```
```