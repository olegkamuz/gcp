package example.gcp;

import com.google.cloud.bigquery.*;

import java.util.List;
import java.util.stream.Collectors;

public class LoadAvroFromGCS {

    public static void runLoadAvroFromGCS() {
        String datasetName = "bq_load_avro";
        String tableName = "avro_all";
        String sourceUri = "gs://spring-bucket-programoleg1/client.avro";
        loadAvroFromGCS(datasetName, tableName, sourceUri);
    }
    public static void runLoadAvroFromGCSNonOptionalFields() {
        String datasetName = "bq_load_avro";
        String tableName = "avro_non_optional";
        String sourceUri = "gs://spring-bucket-programoleg1/client.avro";
        loadAvroNonOptionalFields(datasetName, tableName, sourceUri);
    }

    public static void loadAvroFromGCS(String datasetName, String tableName, String sourceUri) {
        try {
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            TableId tableId = TableId.of(datasetName, tableName);

            LoadJobConfiguration loadConfig = LoadJobConfiguration.of(tableId, sourceUri, FormatOptions.avro());

            Job job = bigquery.create(JobInfo.of(loadConfig));
            job = job.waitFor();
            if (job.isDone()) {
                System.out.println("Avro all from GCS successfully loaded in a table");
            } else {
                System.out.println(
                        "BigQuery was unable to load into the table due to an error:"
                                + job.getStatus().getError());
            }
        } catch (BigQueryException | InterruptedException e) {
            System.out.println("Column not added during load append \n" + e.toString());
        }
    }

    public static void loadAvroNonOptionalFields(String datasetName, String tableName, String sourceUri) {
        try {
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            String FirstTableName = "avro_all";
            Table table = bigquery.getTable(datasetName, FirstTableName);
            Schema schemaAll = table.getDefinition().getSchema();
            List<Field> fieldsAll = schemaAll.getFields();
            List<Field> fieldsNonOptional = fieldsAll.stream()
                    .filter(f -> f.getMode() == Field.Mode.REQUIRED)
                    .collect(Collectors.toList());
            Schema schemaNonOptional = Schema.of(fieldsNonOptional);

            TableId tableId = TableId.of(datasetName, tableName);
            LoadJobConfiguration loadConfig =
                    LoadJobConfiguration.newBuilder(tableId, sourceUri)
                            .setFormatOptions(FormatOptions.avro())
                            .setSchema(schemaNonOptional)
                            .build();

            Job job = bigquery.create(JobInfo.of(loadConfig));
            job = job.waitFor();
            if (job.isDone()) {
                System.out.println("Avro non optional from GCS successfully loaded in a table");
            } else {
                System.out.println(
                        "BigQuery was unable to load into the table due to an error:"
                                + job.getStatus().getError());
            }
        } catch (BigQueryException | InterruptedException e) {
            System.out.println("Column not added during load append \n" + e.toString());
        }
    }

}