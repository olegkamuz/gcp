package example.gcp;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;


import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@AutoConfigureMockMvc
public class LoadAvroFromGCSTest {
    @Autowired
    private LoadAvroFromGCS loadAvroFromGCS;

    private BigQuery bigquery;
    private Storage storage;
    private String dataset;
    private String bucket;
    private String name = "client1.avro";
    private BucketInfo bucketInfo;
    private BlobInfo blobInfo;
    private Blob blob;

    private final Log LOGGER = LogFactory.getLog(LoadController.class);

    @BeforeEach
    void setUp() throws Exception {
        // create mock dataset
        RemoteBigQueryHelper bigqueryHelper = RemoteBigQueryHelper.create();
        bigquery = bigqueryHelper.getOptions().getService();
        dataset = RemoteBigQueryHelper.generateDatasetName();
        bigquery.create(DatasetInfo.newBuilder(dataset).build());

        // create mock bucket
        RemoteStorageHelper helper = RemoteStorageHelper.create();
        storage = helper.getOptions().getService();
        bucket = RemoteStorageHelper.generateBucketName();
        storage.create(BucketInfo.of(bucket));

        // create mock blob
        File avroFile = new ClassPathResource(name).getFile();
        bucketInfo = BucketInfo.of(bucket);
        blobInfo = BlobInfo.newBuilder(bucketInfo, name).build();
        BlobId blobId = BlobId.of(bucket, name);
        blob = storage.create(blobInfo, Files.readAllBytes(avroFile.toPath()));
    }

    @AfterEach
    void tearDown() throws Exception {
        RemoteBigQueryHelper.forceDelete(bigquery, dataset);
        RemoteStorageHelper.forceDelete(storage, bucket, 5, TimeUnit.SECONDS);
    }

    @Test
    public void shouldReturnTrueBigQueryTablesHaveDone() throws Exception {
        loadAvroFromGCS.setBigquery(bigquery);
        loadAvroFromGCS.setStorage(storage);
        loadAvroFromGCS.setBucketName(bucket);
        loadAvroFromGCS.setDatasetName(dataset);

        assertTrue(loadAvroFromGCS.load(name, blob.getGeneration()));
    }

}
