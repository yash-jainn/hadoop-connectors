package com.google.cloud.hadoop.gcsio.benchmarking;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths; // Added import for Paths
import java.util.Collections;

public class SimpleGCSCreate {

    private static final String BUCKET_NAME = "yashjainn-test-bucket";
    private static final String OBJECT_NAME = "simple-test-object"; // A new distinct name
    // IMPORTANT: Set this to the actual path of your Service Account JSON key file
    // Example: "/path/to/your/service-account-key.json"
    private static final String SERVICE_ACCOUNT_KEY_PATH = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");


    public static void main(String[] args) {
        System.out.println("Starting simple GCS object creation test...");
        System.out.println("Bucket: " + BUCKET_NAME + ", Object: " + OBJECT_NAME);
        System.out.println("Using credentials from: " + SERVICE_ACCOUNT_KEY_PATH);

        GoogleCloudStorage gcs = null;
        try {
            // 1. Load credentials from the specified path (or GOOGLE_APPLICATION_CREDENTIALS)
            GoogleCredentials credentials = GoogleCredentials.fromStream(
                    java.nio.file.Files.newInputStream(Paths.get(SERVICE_ACCOUNT_KEY_PATH)));
            System.out.println("GoogleCredentials loaded from path.");

            if (credentials.createScopedRequired()) {
                credentials = credentials.createScoped(
                        Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
                System.out.println("Credentials scoped.");
            }

            // 2. Initialize GCS client options
            GoogleCloudStorageOptions gcsOptions = GoogleCloudStorageOptions.builder()
                    .setAppName("SimpleGCSCreateTest")
                    .build();

            // 3. Initialize the GoogleCloudStorageImpl client
            gcs = GoogleCloudStorageImpl.builder()
                    .setOptions(gcsOptions)
                    .setCredentials(credentials)
                    .build();
            System.out.println("GoogleCloudStorageImpl client initialized.");

            // 4. Verify bucket existence
            StorageResourceId bucketResourceId = new StorageResourceId(BUCKET_NAME);
            GoogleCloudStorageItemInfo bucketInfo = gcs.getItemInfo(bucketResourceId);
            if (!bucketInfo.exists()) {
                System.err.println("ERROR: Bucket '" + BUCKET_NAME + "' does not exist!");
                return; // Exit if bucket doesn't exist
            }
            System.out.println("Bucket '" + BUCKET_NAME + "' confirmed to exist.");

            // 5. Create the test object
            StorageResourceId objectResourceId = new StorageResourceId(BUCKET_NAME, OBJECT_NAME);
            String content = "Hello from SimpleGCSCreate!";
            try (com.google.cloud.hadoop.gcsio.GoogleCloudStorageWriteChannel writeChannel =
                         gcs.create(objectResourceId)) {
                writeChannel.write(ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8)));
            }
            System.out.println("Attempted to create object: gs://" + BUCKET_NAME + "/" + OBJECT_NAME);

            // 6. Verify object existence immediately after creation
            GoogleCloudStorageItemInfo createdObjectInfo = gcs.getItemInfo(objectResourceId);
            if (createdObjectInfo.exists()) {
                System.out.println("SUCCESS: Object '" + OBJECT_NAME + "' IS visible to GCS client.");
                System.out.println("  Size: " + createdObjectInfo.getSize() + " bytes");

                // Optional: Try to delete it immediately to clean up
                System.out.println("Attempting to delete created object...");
                gcs.deleteObjects(Collections.singletonList(objectResourceId));
                System.out.println("Object deleted: " + OBJECT_NAME);
            } else {
                System.err.println("FAILURE: Object '" + OBJECT_NAME + "' IS NOT visible to GCS client after creation attempt.");
            }

        } catch (IOException e) {
            System.err.println("IOException during GCS operation: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("An unexpected error occurred: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (gcs != null) {
                try {
                    gcs.close();
                    System.out.println("GCS client closed.");
                } catch (Exception e) {
                    System.err.println("Error closing GCS client: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }
}