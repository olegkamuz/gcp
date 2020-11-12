package example.gcp;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.sdk.Pipeline;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method.FILE_LOADS;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;

public class LoadAvroFromGCS {
    private static String table1 = "avro_all";
    private static String table2 = "avro_non_optional";
    private static String datasetName = "bq_load_avro";
    private static String bucket = "gs://spring-bucket-programoleg1/";
    private static String projectId = "buoyant-braid-293112";
    private static String region = "europe-west4";
    private static String subscription = "cloud-run-spring";

    private static String bucketName = "spring-bucket-programoleg1";

    private static Schema schemaNonOptional = null;
    private static BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    private static org.apache.avro.Schema schema;

    private static final Log LOGGER = LogFactory.getLog(LoadAvroFromGCS.class);

    public static TableRow convertJsonToTableRow(String json) {
        TableRow row;
        try (InputStream inputStream =
                     new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            row = TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);

        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize json to table row: " + json, e);
        }

        return row;
    }

    private static final SerializableFunction TABLE_ROW_PARSER =
            new SerializableFunction<SpecificRecord, TableRow>() {
                @Override
                public TableRow apply(SpecificRecord specificRecord) {
                    return BigQueryAvroUtils.convertSpecificRecordToTableRow(
                            specificRecord, BigQueryAvroUtils.getTableSchema(Client.SCHEMA$));
                }
            };

    private static final SerializableFunction NON_OPTIONAL_TABLE_ROW_PARSER =
            new SerializableFunction<SpecificRecord, TableRow>() {
                @Override
                public TableRow apply(SpecificRecord specificRecord) {
                    return BigQueryAvroUtils.convertSpecificRecordToTableRow(
                            specificRecord, BigQueryAvroUtils.getOnlyNonOptionalTableSchema(Client.SCHEMA$));
                }
            };

    public static boolean pipeline(String name) {
        TableReference tableReferenceAll = (new TableReference()).setDatasetId(datasetName).setProjectId(projectId).setTableId(table1);
        TableReference tableReferenceNonOptional = (new TableReference()).setDatasetId(datasetName).setProjectId(projectId).setTableId(table2);

        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setStagingLocation("gs://spring-bucket-programoleg1/staging");
        options.setTempLocation("gs://spring-bucket-programoleg1/tmp");
        options.setProject(projectId);
        options.setRunner(DataflowRunner.class);
        options.setRegion(region);

        Pipeline pipeline = Pipeline.create(options);

        PCollection<Client> records = pipeline.apply("Read Avro files", AvroIO.read(Client.class).from("gs://" + bucketName + "/" + name + ".avro"));

        TableSchema ts = BigQueryAvroUtils.getTableSchema(Client.SCHEMA$);
        TableSchema tsno = BigQueryAvroUtils.getOnlyNonOptionalTableSchema(Client.SCHEMA$);

        records.apply("Write all to BigQuery",
                BigQueryIO.writeTableRows()
                        .to(tableReferenceAll)
                        .withSchema(ts)
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND)
                        .withFormatFunction(TABLE_ROW_PARSER));

        records.apply("Write non optional to BigQuery",
                BigQueryIO.writeTableRows()
                        .to(tableReferenceNonOptional)
                        .withSchema(tsno)
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND)
                        .withFormatFunction(NON_OPTIONAL_TABLE_ROW_PARSER));

        pipeline.run().waitUntilFinish();
        return true;
    }

    public static boolean load(String name, Long generation) {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        LOGGER.warn("storage: " + storage);
        BlobId blobId = BlobId.of(bucketName, name, generation);
        LOGGER.warn("blobId: " + blobId);
        Blob blob = storage.get(blobId);
        LOGGER.warn("blob: " + blob);
        String value = new String(blob.getContent());
        System.out.println("VALUE: " + value);
        SeekableByteArrayInput input = new SeekableByteArrayInput(blob.getContent());

        try {
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(input, datumReader);
            schema = dataFileReader.getSchema();
            return pipeline(name);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        return true;

    }

    public static boolean runLoadAvroFromGCS(String name) {
        String sourceUri = bucket + name;
        return loadAvroFromGCS(datasetName, table1, sourceUri);
    }

    public static boolean runLoadAvroFromGCSNonOptionalFields(String name) {
        String sourceUri = bucket + name;
        return loadAvroNonOptionalFields(datasetName, table2, sourceUri);
    }

    private static boolean loadAvroFromGCS(String datasetName, String tableName, String sourceUri) {
        try {
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

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
            if (schemaNonOptional == null) {
                initNotOptionalScheme();
            }

            TableId tableId = TableId.of(datasetName, tableName);
            LoadJobConfiguration loadConfig =
                    LoadJobConfiguration.newBuilder(tableId, sourceUri)
                            .setFormatOptions(FormatOptions.avro())
                            .setSchema(schemaNonOptional)
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

    private static void initNotOptionalScheme() {
        Table table = bigquery.getTable(datasetName, table1);
        Schema schemaAll = table.getDefinition().getSchema();
        List<Field> fieldsAll = schemaAll.getFields();
        List<Field> fieldsNonOptional = fieldsAll.stream()
                .filter(f -> f.getMode() == Field.Mode.REQUIRED)
                .collect(Collectors.toList());
        Schema schemaNonOptional = Schema.of(fieldsNonOptional);
    }
}