package example.gcp;

import com.google.cloud.bigquery.*;

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
        Schema schema =
                Schema.of(
                        Field.of("id", StandardSQLTypeName.INT64),
                        Field.of("name", StandardSQLTypeName.STRING));
        loadAvroNonOptionalFields(datasetName, tableName, sourceUri, schema);
    }

    public static void loadAvroFromGCS(String datasetName, String tableName, String sourceUri) {
        try {
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            TableId tableId = TableId.of(datasetName, tableName);
            LoadJobConfiguration loadConfig =
                    LoadJobConfiguration.of(tableId, sourceUri, FormatOptions.avro());

            Job job = bigquery.create(JobInfo.of(loadConfig));
            job = job.waitFor();
            if (job.isDone()) {
                System.out.println("Avro from GCS successfully loaded in a table");
            } else {
                System.out.println(
                        "BigQuery was unable to load into the table due to an error:"
                                + job.getStatus().getError());
            }
        } catch (BigQueryException | InterruptedException e) {
            System.out.println("Column not added during load append \n" + e.toString());
        }
    }

    public static void loadAvroNonOptionalFields(String datasetName, String tableName, String sourceUri, Schema schema) {
        try {
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            TableId tableId = TableId.of(datasetName, tableName);
            LoadJobConfiguration loadConfig =
                    LoadJobConfiguration.newBuilder(tableId, sourceUri)
                            .setFormatOptions(FormatOptions.avro())
                            .setSchema(schema)
                            .build();

            Job job = bigquery.create(JobInfo.of(loadConfig));
            job = job.waitFor();
            if (job.isDone()) {
                System.out.println("Avro from GCS successfully loaded in a table");
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