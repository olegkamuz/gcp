package example.gcp.service.utils;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import example.gcp.Client;
import example.gcp.service.LoadAvroFromGCS;
import example.gcp.service.utils.AvroToBigQueryHelper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
@Data
@Slf4j
public class LoadAvroFromGCSImpl implements LoadAvroFromGCS {
    private static final String TABLE_AVRO_ALL = "avro_all";
    private static final String TABLE_AVRO_NON_OPTIONAL = "avro_non_optional";
    private String datasetName = "bq_load_avro";
    private String bucketName = "spring-bucket-programoleg1";
    private static Schema schemaAll = null;
    private static com.google.cloud.bigquery.Schema schemaBQNonOptional = null;
    private BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    private Storage storage = StorageOptions.getDefaultInstance().getService();

    private Blob getBlob(String name, Long generation) {
        BlobId blobId = BlobId.of(bucketName, name, generation);
        log.info("blobId: " + blobId);
        return storage.get(blobId);
    }

    public boolean load(String name, Long generation) {
        Blob blob = getBlob(name, generation);
        log.info("blob: " + blob);

        SeekableByteArrayInput input = new SeekableByteArrayInput(blob.getContent());

        schemaAll = getSchemaAll(input);
        schemaBQNonOptional = getSchemaBQNonOptional();

        return runLoadAvroFromGCS(name) && runLoadAvroFromGCSNonOptionalFields(name) && deleteObject(name);
    }

    private com.google.cloud.bigquery.Schema getSchemaBQNonOptional() {
        List<com.google.cloud.bigquery.Field> fieldsBQ = new ArrayList<>();
        for (Schema.Field f : schemaAll.getFields()) {
            if (f.schema().isNullable()) {
                continue;
            }
            com.google.cloud.bigquery.Field fieldBQ = com.google.cloud.bigquery.Field.of(f.name(), AvroToBigQueryHelper.convertAvroFieldTypeToBigQueryFieldType(f));
            fieldsBQ.add(fieldBQ);
        }
        return com.google.cloud.bigquery.Schema.of(fieldsBQ);
    }

    private Schema getSchemaAll(SeekableByteArrayInput input) {
        try {
            DatumReader<Client> datumReader = new SpecificDatumReader<>();
            DataFileReader<Client> dataFileReader = new DataFileReader<>(input, datumReader);
            return dataFileReader.getSchema();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    private boolean runLoadAvroFromGCS(String name) {
        String sourceUri = "gs://" + bucketName + "/" + name;
        return loadAvroFromGCS(datasetName, TABLE_AVRO_ALL, sourceUri);
    }

    private boolean runLoadAvroFromGCSNonOptionalFields(String name) {
        String sourceUri = "gs://" + bucketName + "/" + name;
        return loadAvroNonOptionalFields(datasetName, TABLE_AVRO_NON_OPTIONAL, sourceUri);
    }

    private boolean loadAvroFromGCS(String datasetName, String tableName, String sourceUri) {
        try {
            TableId tableId = TableId.of(datasetName, tableName);

            LoadJobConfiguration loadConfig = LoadJobConfiguration.of(tableId, sourceUri, FormatOptions.avro());

            Job job = bigquery.create(JobInfo.of(loadConfig));
            job = job.waitFor();
            if (job.isDone()) {
                log.info("Avro all from GCS successfully loaded in a table");
                return true;
            } else {
                log.warn("BigQuery was unable to load into the table due to an error:" + job.getStatus().getError());
                return false;
            }
        } catch (BigQueryException | InterruptedException e) {
            log.warn("Column not added during load append \n" + e.toString());
            return false;
        }
    }

    private boolean loadAvroNonOptionalFields(String datasetName, String tableName, String sourceUri) {
        try {
            if (schemaBQNonOptional == null) {
                schemaBQNonOptional = getSchemaBQNonOptional();
            }

            TableId tableId = TableId.of(datasetName, tableName);
            LoadJobConfiguration loadConfig = LoadJobConfiguration.newBuilder(tableId, sourceUri)
                    .setFormatOptions(FormatOptions.avro())
                    .setSchema(schemaBQNonOptional)
                    .build();

            Job job = bigquery.create(JobInfo.of(loadConfig));
            job = job.waitFor();
            if (job.isDone()) {
                log.info("Avro non optional from GCS successfully loaded in a table");
                return true;
            } else {
                log.warn("BigQuery was unable to load into the table due to an error:" + job.getStatus().getError());
                return false;
            }
        } catch (BigQueryException | InterruptedException e) {
            log.warn("Column not added during load append \n" + e.toString());
            return false;
        }
    }

    private boolean deleteObject(String objectName) {
        if (storage.delete(bucketName, objectName)) {
            log.info("Object " + objectName + " was deleted from " + bucketName);
            return true;
        }
        log.warn("Deletion unsuccessful");
        return false;
    }
}