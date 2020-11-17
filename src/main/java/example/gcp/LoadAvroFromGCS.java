package example.gcp;

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
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

public class LoadAvroFromGCS {
    private static final Log LOGGER = LogFactory.getLog(LoadAvroFromGCS.class);
    private static String table_avro_all = "avro_all";
    private static String table_avro_non_optional = "avro_non_optional";
    private static String datasetName = "bq_load_avro";
    private static String bucket = "gs://spring-bucket-programoleg1/";
    private static String bucketName = "spring-bucket-programoleg1";
    private static Schema schemaAll = null;
    private static com.google.cloud.bigquery.Schema schemaBQNonOptional = null;
    private static BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    private static Storage storage = StorageOptions.getDefaultInstance().getService();

    private static Blob getBlob(String name, Long generation) {
        BlobId blobId = BlobId.of(bucketName, name, generation);
        LOGGER.info("blobId: " + blobId);
        return storage.get(blobId);
    }

    public static boolean load(String name, Long generation) {
        Blob blob = getBlob(name, generation);
        LOGGER.info("blob: " + blob);

        SeekableByteArrayInput input = new SeekableByteArrayInput(blob.getContent());

        schemaAll = getSchemaAll(input);
        schemaBQNonOptional = getSchemaBQNonOptional();

        return runLoadAvroFromGCS(name) && runLoadAvroFromGCSNonOptionalFields(name) && deleteObject(name);
    }

    private static com.google.cloud.bigquery.Schema getSchemaBQNonOptional() {
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

    private static Schema getSchemaAll(SeekableByteArrayInput input) {
        try {
            DatumReader<Client> datumReader = new SpecificDatumReader<>();
            DataFileReader<Client> dataFileReader = new DataFileReader<>(input, datumReader);
            return dataFileReader.getSchema();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }


    private static boolean runLoadAvroFromGCS(String name) {
        String sourceUri = bucket + name;
        return loadAvroFromGCS(datasetName, table_avro_all, sourceUri);
    }

    private static boolean runLoadAvroFromGCSNonOptionalFields(String name) {
        String sourceUri = bucket + name;
        return loadAvroNonOptionalFields(datasetName, table_avro_non_optional, sourceUri);
    }

    private static boolean loadAvroFromGCS(String datasetName, String tableName, String sourceUri) {
        try {
            TableId tableId = TableId.of(datasetName, tableName);

            LoadJobConfiguration loadConfig = LoadJobConfiguration.of(tableId, sourceUri, FormatOptions.avro());

            Job job = bigquery.create(JobInfo.of(loadConfig));
            job = job.waitFor();
            if (job.isDone()) {
                LOGGER.info("Avro all from GCS successfully loaded in a table");
                return true;
            } else {
                LOGGER.warn("BigQuery was unable to load into the table due to an error:" + job.getStatus().getError());
                return false;
            }
        } catch (BigQueryException | InterruptedException e) {
            LOGGER.warn("Column not added during load append \n" + e.toString());
            return false;
        }
    }

    private static boolean loadAvroNonOptionalFields(String datasetName, String tableName, String sourceUri) {
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
                LOGGER.info("Avro non optional from GCS successfully loaded in a table");
                return true;
            } else {
                LOGGER.warn("BigQuery was unable to load into the table due to an error:" + job.getStatus().getError());
                return false;
            }
        } catch (BigQueryException | InterruptedException e) {
            LOGGER.warn("Column not added during load append \n" + e.toString());
            return false;
        }
    }

    private static boolean deleteObject(String objectName) {
        if (storage.delete(bucketName, objectName)) {
            LOGGER.info("Object " + objectName + " was deleted from " + bucketName);
            return true;
        }
        LOGGER.warn("Deletion unsuccessful");
        return false;
    }
}