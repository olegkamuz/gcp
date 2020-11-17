package example.gcp;

import com.google.cloud.bigquery.StandardSQLTypeName;
import org.apache.avro.Schema;

public class AvroToBigQueryHelper {

    public static StandardSQLTypeName convertAvroFieldTypeToBigQueryFieldType(Schema.Field field) {
        Schema.Type type = field.schema().getType();
        switch (type) {
            case INT:
            case LONG:
                return StandardSQLTypeName.INT64;
            case BOOLEAN:
                return StandardSQLTypeName.BOOL;
            case FLOAT:
            case DOUBLE:
                return StandardSQLTypeName.FLOAT64;
            default:
                return StandardSQLTypeName.STRING;
        }
    }
}