package example.gcp;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class ManageStorageObjects {
    private static final Storage storage = StorageOptions.newBuilder().setProjectId("buoyant-braid-293112").build().getService();
    private static final Bucket bucket = storage.get("spring-bucket-programoleg1");

    public static Page<Blob> listObjects() {
        Page<Blob> blobs = bucket.list();
        return blobs;
    }
    public static void deleteObject(String objectName) {
        storage.delete(bucket.getName(), objectName);
        System.out.println("Object " + objectName + " was deleted from " + bucket.getName());
    }
}