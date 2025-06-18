// In a new file: com.google.cloud.hadoop.fs.gcs.benchmarking.BenchmarkingGoogleHadoopFileSystem.java

package com.google.cloud.hadoop.fs.gcs.benchmarking;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import com.google.common.flogger.GoogleLogger; // Assuming you use this

public abstract class BenchmarkingGoogleHadoopFileSystem extends FileSystem {

    private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
    private GoogleHadoopFileSystem delegateFs;

    // Constructor to wrap the actual FileSystem
    public BenchmarkingGoogleHadoopFileSystem() {
        // This will be initialized in the initialize method
    }

    @Override
    public void initialize(URI path, Configuration config) throws IOException {
        // Only initialize the delegate if benchmarking is NOT enabled *OR* if it's already running a benchmark.
        // This prevents recursive initialization if the benchmark itself needs the FS.
        if (!BenchmarkState.IS_BENCHMARKING_ENABLED.get() || BenchmarkState.IS_IN_BENCHMARK_EXECUTION.get()) {
            delegateFs = new GoogleHadoopFileSystem();
            delegateFs.initialize(path, config);
            setConf(config); // Ensure this wrapper also has the correct config
        } else {
            // If benchmarking is enabled and not already running, we won't fully initialize
            // the delegate here, as the benchmark will take over.
            // We still need to set the config for this wrapper, though.
            setConf(config);
            // This is a crucial point: if you want the benchmark to *replace* the operation,
            // then the delegate won't be initialized until *after* the benchmark.
            // For JMH benchmarks, they typically run in a separate JVM, so the delegate
            // is effectively created and discarded for each benchmark run.
        }

        // ONE-TIME INITIALIZATION of the global benchmark toggle.
        // This logic should ideally be outside, perhaps in a custom FileSystem factory,
        // but for now, keep it here as per your original code.
        if (!GoogleHadoopFileSystem.benchmarkToggleInitialized) { // Accessing static member
            boolean isEnabledInConfig = config.getBoolean(GoogleHadoopFileSystemConfiguration.GCS_CONNECTOR_BENCHMARK_ENABLE.getKey(), false);
            BenchmarkState.IS_BENCHMARKING_ENABLED.set(isEnabledInConfig);
            GoogleHadoopFileSystem.benchmarkToggleInitialized = true;
            logger.atInfo().log("Benchmark Toggle initialized to: %b in wrapper", isEnabledInConfig);
        }
    }

    // Helper method to ensure delegate is initialized when needed for actual operations
    private GoogleHadoopFileSystem ensureDelegateInitialized() throws IOException {
        if (delegateFs == null) {
            // This path should ideally not be taken if initialize() is called correctly
            // or if the benchmark logic correctly handles the delegate.
            // If the benchmark *replaces* the call, this won't be needed during benchmark execution.
            // If the benchmark *then* calls the delegate, it needs to be initialized.
            // For this wrapper, we assume if benchmarking is not active, the delegate is used.
            if (getConf() == null) {
                throw new IllegalStateException("Configuration not set for BenchmarkingGoogleHadoopFileSystem.");
            }
            logger.atWarning().log("Delegate FS was null, initializing on demand. This should ideally be done in initialize().");
            delegateFs = new GoogleHadoopFileSystem();
            delegateFs.initialize(getUri(), getConf());
        }
        return delegateFs;
    }

    private <T> T handleBenchmarkOrDelegate(
            Path hadoopPath,
            Runnable benchmarkAction,
            CallableRaisingIOE<T> delegateAction) throws IOException {

        if (BenchmarkState.IS_BENCHMARKING_ENABLED.compareAndSet(true, false)
                && !BenchmarkState.IS_IN_BENCHMARK_EXECUTION.get()) {
            System.out.println("======================================================");
            System.out.println("  JMH BENCHMARK TRIGGERED!");
            System.out.println("  Path: " + hadoopPath);
            System.out.println("  (Main benchmark toggle has now been permanently disabled)");
            System.out.println("======================================================");

            try {
                BenchmarkState.IS_IN_BENCHMARK_EXECUTION.set(true);
                benchmarkAction.run();
            } catch (Exception e) { // Catch Exception to re-enable the toggle
                BenchmarkState.IS_BENCHMARKING_ENABLED.set(true); // Reset on failure
                System.err.println("JMH benchmark failed to run: " + e.getMessage());
                throw new IOException("Failed to run JMH benchmark", e);
            } finally {
                BenchmarkState.IS_IN_BENCHMARK_EXECUTION.set(false);
                BenchmarkState.IS_BENCHMARKING_ENABLED.compareAndSet(false, true); // Re-enable for subsequent ops
            }

            System.out.println("======================================================");
            System.out.println("  JMH BENCHMARK FINISHED.          ");
            System.out.println("======================================================");

            // After the benchmark, we MUST proceed with the original operation
            // This ensures the calling application gets a valid result.
            return delegateAction.call();
        } else {
            // If benchmarking is not enabled or already running a benchmark, just delegate.
            return delegateAction.call();
        }
    }

    @Override
    public FSDataOutputStream create(
            Path hadoopPath,
            FsPermission permission,
            boolean overwrite,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progress)
            throws IOException {

        return handleBenchmarkOrDelegate(
                hadoopPath,
                () -> {
                    try {
                        GCSCreateBenchmark.runBenchmark(hadoopPath);
                    } catch (IOException e) {
                        throw new RuntimeException(e); // JMH expects RuntimeException if it fails
                    }
                },
                () -> ensureDelegateInitialized().create(hadoopPath, permission, overwrite, bufferSize, replication, blockSize, progress)
        );
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return handleBenchmarkOrDelegate(
                src,
                () -> {
                    try {
                        GCSRenameBenchmark.runBenchmark(src, dst); // Assuming you create this
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                },
                () -> ensureDelegateInitialized().rename(src, dst)
        );
    }

    @Override
    public FSDataInputStream open(Path hadoopPath, int bufferSize) throws IOException {
        return handleBenchmarkOrDelegate(
                hadoopPath,
                () -> {
                    try {
                        GCSOpenBenchmark.runBenchmark(hadoopPath); // Assuming you create this
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                },
                () -> ensureDelegateInitialized().open(hadoopPath, bufferSize)
        );
    }

    // Implement all other FileSystem methods by delegating to delegateFs
    @Override
    public URI getUri() {
        return delegateFs != null ? delegateFs.getUri() : null; // May be null during early init
    }

    @Override
    public Path getWorkingDirectory() {
        return ensureDelegateInitialized().getWorkingDirectory();
    }

    @Override
    public void setWorkingDirectory(Path newDir) {
        ensureDelegateInitialized().setWorkingDirectory(newDir);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        return handleBenchmarkOrDelegate(
                f,
                () -> {
                    try {
                        GCSListStatusBenchmark.runBenchmark(f); // Assuming you create this
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                },
                () -> ensureDelegateInitialized().listStatus(f)
        );
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        return handleBenchmarkOrDelegate(
                f,
                () -> {
                    try {
                        GCSDeleteBenchmark.runBenchmark(f, recursive); // Assuming you create this
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                },
                () -> ensureDelegateInitialized().delete(f, recursive)
        );
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        return handleBenchmarkOrDelegate(
                f,
                () -> {
                    try {
                        GCSGetFileStatusBenchmark.runBenchmark(f); // Assuming you create this
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                },
                () -> ensureDelegateInitialized().getFileStatus(f)
        );
    }

    @Override
    public void close() throws IOException {
        if (delegateFs != null) {
            delegateFs.close();
        }
        super.close();
    }
}