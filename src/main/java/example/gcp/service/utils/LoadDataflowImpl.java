package example.gcp.service.utils;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Schema;
import example.gcp.Client;
import example.gcp.service.LoadAvroFromGCS;
import org.apache.avro.specific.SpecificRecord;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

public class LoadDataflowImpl implements LoadAvroFromGCS {

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
            (SerializableFunction<SpecificRecord, TableRow>) specificRecord -> BigQueryAvroUtils.convertSpecificRecordToTableRow(
                    specificRecord, BigQueryAvroUtils.getTableSchema(Client.SCHEMA$));

    private static final SerializableFunction NON_OPTIONAL_TABLE_ROW_PARSER =
            (SerializableFunction<SpecificRecord, TableRow>) specificRecord -> BigQueryAvroUtils.convertSpecificRecordToTableRow(
                    specificRecord, BigQueryAvroUtils.getOnlyNonOptionalTableSchema(Client.SCHEMA$));

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

    @Override
    public boolean load(String name, Long generation) {
        return pipeline(name);
    }
}
