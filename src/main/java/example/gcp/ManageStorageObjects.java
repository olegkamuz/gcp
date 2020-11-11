package example.gcp;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ManageStorageObjects {
    private static final Storage storage = StorageOptions.newBuilder().setProjectId("buoyant-braid-293112").build().getService();
    private static final Bucket bucket = storage.get("spring-bucket-programoleg1");
    private static final Log LOGGER = LogFactory.getLog(ManageStorageObjects.class);

    public static Page<Blob> listObjects() {
        Page<Blob> blobs = bucket.list();
        return blobs;
    }
    public static void deleteObject(String objectName) {
        if (storage.delete(bucket.getName(), objectName)) {
            LOGGER.info("Object " + objectName + " was deleted from " + bucket.getName());
        } else {
            LOGGER.warn("Deletion unsuccessful");
        }
    }
}