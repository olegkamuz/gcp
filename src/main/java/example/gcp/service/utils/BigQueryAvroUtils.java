package example.gcp.service.utils;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.joda.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.format.ISODateTimeFormat;
import static java.time.temporal.ChronoField.*;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMultimap;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import com.google.common.io.BaseEncoding;

public class BigQueryAvroUtils {
    private BigQueryAvroUtils() {
    }

    public static class BigQueryAvroMapper {

        private BigQueryAvroMapper() {
        }

        private static final ImmutableMultimap<String, Type> BIG_QUERY_TO_AVRO_TYPES =
                ImmutableMultimap.<String, Type>builder()
                        .put("STRING", Type.STRING)
                        .put("GEOGRAPHY", Type.STRING)
                        .put("BYTES", Type.BYTES)
                        .put("INTEGER", Type.INT)
                        .put("FLOAT", Type.FLOAT)
                        .put("FLOAT64", Type.DOUBLE)
                        .put("NUMERIC", Type.BYTES)
                        .put("BOOLEAN", Type.BOOLEAN)
                        .put("INT64", Type.LONG)
                        .put("TIMESTAMP", Type.LONG)
                        .put("RECORD", Type.RECORD)
                        .put("DATE", Type.INT)
                        .put("DATETIME", Type.STRING)
                        .put("TIME", Type.LONG)
                        .put("STRUCT", Type.RECORD)
                        .put("ARRAY", Type.ARRAY)
                        .put("UNION", Type.UNION)
                        .build();

        private static final ImmutableMultimap<Type, String> AVRO_TYPES_TO_BIG_QUERY =
                BIG_QUERY_TO_AVRO_TYPES.inverse();

        public static String getBigQueryTypes(Type type) {
            return AVRO_TYPES_TO_BIG_QUERY.get(type).iterator().next();
        }

        public static ImmutableCollection<Type> getAvroTypes(String bqType) {
            return BIG_QUERY_TO_AVRO_TYPES.get(bqType);
        }
    }

    /**
     * Formats BigQuery seconds-since-epoch into String matching JSON export. Thread-safe and
     * immutable.
     */
    private static final DateTimeFormatter DATE_AND_SECONDS_FORMATTER =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();

    @VisibleForTesting
    static String formatTimestamp(DateTime ts) {
        String dayAndTime = ts.toString(DATE_AND_SECONDS_FORMATTER);
        return String.format("%s UTC", dayAndTime);
    }

    /**
     * This method formats a BigQuery DATE value into a String matching the format used by JSON
     * export. Date records are stored in "days since epoch" format, and BigQuery uses the proleptic
     * Gregorian calendar.
     */
    private static String formatDate(LocalDate date) {
        return date.toString(ISODateTimeFormat.date());
    }

    private static final java.time.format.DateTimeFormatter ISO_LOCAL_TIME_FORMATTER_MICROS =
            new DateTimeFormatterBuilder()
                    .appendValue(HOUR_OF_DAY, 2)
                    .appendLiteral(':')
                    .appendValue(MINUTE_OF_HOUR, 2)
                    .appendLiteral(':')
                    .appendValue(SECOND_OF_MINUTE, 2)
                    .appendLiteral('.')
                    .appendFraction(NANO_OF_SECOND, 6, 6, false)
                    .toFormatter();

    private static final java.time.format.DateTimeFormatter ISO_LOCAL_TIME_FORMATTER_MILLIS =
            new DateTimeFormatterBuilder()
                    .appendValue(HOUR_OF_DAY, 2)
                    .appendLiteral(':')
                    .appendValue(MINUTE_OF_HOUR, 2)
                    .appendLiteral(':')
                    .appendValue(SECOND_OF_MINUTE, 2)
                    .appendLiteral('.')
                    .appendFraction(NANO_OF_SECOND, 3, 3, false)
                    .toFormatter();

    private static final java.time.format.DateTimeFormatter ISO_LOCAL_TIME_FORMATTER_SECONDS =
            new DateTimeFormatterBuilder()
                    .appendValue(HOUR_OF_DAY, 2)
                    .appendLiteral(':')
                    .appendValue(MINUTE_OF_HOUR, 2)
                    .appendLiteral(':')
                    .appendValue(SECOND_OF_MINUTE, 2)
                    .toFormatter();

    /**
     * This method formats a BigQuery TIME value into a String matching the format used by JSON
     * export. Time records are stored in "microseconds since midnight" format.
     */
    private static String formatTime(long timeMicros) {
        java.time.format.DateTimeFormatter formatter;
        if (timeMicros % 1000000 == 0) {
            formatter = ISO_LOCAL_TIME_FORMATTER_SECONDS;
        } else if (timeMicros % 1000 == 0) {
            formatter = ISO_LOCAL_TIME_FORMATTER_MILLIS;
        } else {
            formatter = ISO_LOCAL_TIME_FORMATTER_MICROS;
        }
        return LocalTime.ofNanoOfDay(timeMicros * 1000).format(formatter);
    }

    /**
     * Utility function to convert from an Avro {@link GenericRecord} to a BigQuery {@link TableRow}.
     *
     * <p>See <a href="https://cloud.google.com/bigquery/exporting-data-from-bigquery#config">"Avro
     * format"</a> for more information.
     */
    static TableRow convertGenericRecordToTableRow(GenericRecord record, TableSchema schema) {
        return convertGenericRecordToTableRow(record, schema.getFields());
    }

    private static TableRow convertGenericRecordToTableRow(
            GenericRecord record, List<TableFieldSchema> fields) {
        TableRow row = new TableRow();
        for (TableFieldSchema subSchema : fields) {
            // Per https://cloud.google.com/bigquery/docs/reference/v2/tables#schema, the name field
            // is required, so it may not be null.
            Field field = record.getSchema().getField(subSchema.getName());
            Object convertedValue =
                    getTypedCellValue(field.schema(), subSchema, record.get(field.name()));
            if (convertedValue != null) {
                // To match the JSON files exported by BigQuery, do not include null values in the output.
                row.set(field.name(), convertedValue);
            }
        }

        return row;
    }

    public static TableRow convertSpecificRecordToTableRow(SpecificRecord record, TableSchema schema) {
        return convertSpecificRecordToTableRow(record, schema.getFields());
    }

    // [START convert_tablerow]
    private static TableRow convertSpecificRecordToTableRow(SpecificRecord record, List<TableFieldSchema> fields) {
        TableRow row = new TableRow();
        for (TableFieldSchema subSchema : fields) {
            // Per https://cloud.google.com/bigquery/docs/reference/v2/tables#schema, the name field
            // is required, so it may not be null.
            Field field = record.getSchema().getField(subSchema.getName());
            if (field == null || field.name() == null) {
                continue;
            }
            Object convertedValue = getTypedCellValue(field.schema(), subSchema, record.get(field.pos()));
            if (convertedValue != null) {
                // To match the JSON files exported by BigQuery, do not include null values in the output.
                row.set(field.name(), convertedValue);
            }
        }

        return row;
    }
    // [END convert_tablerow]

    @Nullable
    private static Object getTypedCellValue(Schema schema, TableFieldSchema fieldSchema, Object v) {
        // Per https://cloud.google.com/bigquery/docs/reference/v2/tables#schema, the mode field
        // is optional (and so it may be null), but defaults to "NULLABLE".
        String mode = firstNonNull(fieldSchema.getMode(), "NULLABLE");
        switch (mode) {
            case "REQUIRED":
                return convertRequiredField(schema.getType(), schema.getLogicalType(), fieldSchema, v);
            case "REPEATED":
                return convertRepeatedField(schema, fieldSchema, v);
            case "NULLABLE":
                return convertNullableField(schema, fieldSchema, v);
            default:
                throw new UnsupportedOperationException(
                        "Parsing a field with BigQuery field schema mode " + fieldSchema.getMode());
        }
    }

    private static List<Object> convertRepeatedField(
            Schema schema, TableFieldSchema fieldSchema, Object v) {
        Type arrayType = schema.getType();
        verify(
                arrayType == Type.ARRAY,
                "BigQuery REPEATED field %s should be Avro ARRAY, not %s",
                fieldSchema.getName(),
                arrayType);
        // REPEATED fields are represented as Avro arrays.
        if (v == null) {
            // Handle the case of an empty repeated field.
            return new ArrayList<>();
        }
        @SuppressWarnings("unchecked")
        List<Object> elements = (List<Object>) v;
        ArrayList<Object> values = new ArrayList<>();
        Type elementType = schema.getElementType().getType();
        LogicalType elementLogicalType = schema.getElementType().getLogicalType();
        for (Object element : elements) {
            values.add(convertRequiredField(elementType, elementLogicalType, fieldSchema, element));
        }
        return values;
    }

    private static Object convertRequiredField(Type avroType, LogicalType avroLogicalType, TableFieldSchema fieldSchema, Object v) {
        // REQUIRED fields are represented as the corresponding Avro types. For example, a BigQuery
        // INTEGER type maps to an Avro LONG type.
        checkNotNull(v, "REQUIRED field %s should not be null", fieldSchema.getName());
        // Per https://cloud.google.com/bigquery/docs/reference/v2/tables#schema, the type field
        // is required, so it may not be null.
        String bqType = fieldSchema.getType();
        ImmutableCollection<Type> expectedAvroTypes = BigQueryAvroMapper.getAvroTypes(bqType);
        verifyNotNull(expectedAvroTypes, "Unsupported BigQuery type: %s", bqType);
        verify(
                expectedAvroTypes.contains(avroType),
                "Expected Avro schema types %s for BigQuery %s field %s, but received %s",
                expectedAvroTypes,
                bqType,
                fieldSchema.getName(),
                avroType);
        // For historical reasons, don't validate avroLogicalType except for with NUMERIC.
        // BigQuery represents NUMERIC in Avro format as BYTES with a DECIMAL logical type.
        switch (bqType) {
            case "STRING":
            case "DATETIME":
            case "GEOGRAPHY":
                // Avro will use a CharSequence to represent String objects, but it may not always use
                // java.lang.String; for example, it may prefer org.apache.avro.util.Utf8.
                verify(v instanceof CharSequence, "Expected CharSequence (String), got %s", v.getClass());
                return v.toString();
            case "DATE":
                if (avroType == Type.INT) {
                    verify(v instanceof org.joda.time.LocalDate, "Expected org.joda.time.LocalDate, got %s",
                            v.getClass());
                    verifyNotNull(avroLogicalType, "Expected Date logical type");
                    verify(avroLogicalType instanceof LogicalTypes.Date, "Expected Date logical type");
                    return formatDate((LocalDate) v);
                } else {
                    verify(v instanceof CharSequence, "Expected CharSequence (String), got %s", v.getClass());
                    return v.toString();
                }
            case "TIME":
                if (avroType == Type.LONG) {
                    verify(v instanceof Long, "Expected Long, got %s", v.getClass());
                    verifyNotNull(avroLogicalType, "Expected TimeMicros logical type");
                    verify(
                            avroLogicalType instanceof LogicalTypes.TimeMicros,
                            "Expected TimeMicros logical type");
                    return formatTime((Long) v);
                } else {
                    verify(v instanceof CharSequence, "Expected CharSequence (String), got %s", v.getClass());
                    return v.toString();
                }
            case "INTEGER":
                verify(v instanceof Integer, "Expected Integer, got %s", v.getClass());
                return v;
            case "INT64":
            case "LONG":
                verify(v instanceof Long, "Expected Long, got %s", v.getClass());
                return v;
            case "FLOAT64":
                verify(v instanceof Double, "Expected Dpuble, got %s", v.getClass());
                return v;
            case "FLOAT":
                verify(v instanceof Float, "Expected Float, got %s", v.getClass());
                return v;
            case "NUMERIC":
                // NUMERIC data types are represented as BYTES with the DECIMAL logical type. They are
                // converted back to Strings with precision and scale determined by the logical type.
                verify(v instanceof ByteBuffer, "Expected ByteBuffer, got %s", v.getClass());
                verifyNotNull(avroLogicalType, "Expected Decimal logical type");
                verify(avroLogicalType instanceof LogicalTypes.Decimal, "Expected Decimal logical type");
                BigDecimal numericValue =
                        new Conversions.DecimalConversion()
                                .fromBytes((ByteBuffer) v, Schema.create(avroType), avroLogicalType);
                return numericValue.toString();
            case "BOOLEAN":
                verify(v instanceof Boolean, "Expected Boolean, got %s", v.getClass());
                return v;
            case "TIMESTAMP":
                // TIMESTAMP data types are represented as JodaTime DateTime type.
                verify(v instanceof DateTime, "Expected Long, got %s", v.getClass());
                return formatTimestamp((DateTime) v);
            case "STRUCT":
                verify(v instanceof SpecificRecord, "Expected SpecificRecord, got %s", v.getClass());
                return convertSpecificRecordToTableRow((SpecificRecord) v, fieldSchema.getFields());
            case "RECORD":
                verify(v instanceof GenericRecord, "Expected GenericRecord, got %s", v.getClass());
                return convertGenericRecordToTableRow((GenericRecord) v, fieldSchema.getFields());
            case "BYTES":
                verify(v instanceof ByteBuffer, "Expected ByteBuffer, got %s", v.getClass());
                ByteBuffer byteBuffer = (ByteBuffer) v;
                byte[] bytes = new byte[byteBuffer.limit()];
                byteBuffer.get(bytes);
                return BaseEncoding.base64().encode(bytes);
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unexpected BigQuery field schema type %s for field named %s",
                                fieldSchema.getType(), fieldSchema.getName()));
        }
    }

    @Nullable
    private static Object convertNullableField(
            Schema avroSchema, TableFieldSchema fieldSchema, Object v) {
        // NULLABLE fields are represented as an Avro Union of the corresponding type and "null".
        verify(
                avroSchema.getType() == Type.UNION,
                "Expected Avro schema type UNION, not %s, for BigQuery NULLABLE field %s",
                avroSchema.getType(),
                fieldSchema.getName());
        List<Schema> unionTypes = avroSchema.getTypes();
        verify(
                unionTypes.size() == 2,
                "BigQuery NULLABLE field %s should be an Avro UNION of NULL and another type, not %s",
                fieldSchema.getName(),
                unionTypes);

        if (v == null) {
            return null;
        }

        Type firstType = unionTypes.get(0).getType();
        if (!firstType.equals(Type.NULL)) {
            return convertRequiredField(firstType, unionTypes.get(0).getLogicalType(), fieldSchema, v);
        }
        return convertRequiredField(
                unionTypes.get(1).getType(), unionTypes.get(1).getLogicalType(), fieldSchema, v);
    }

    static Schema toGenericAvroSchema(String schemaName, List<TableFieldSchema> fieldSchemas) {
        List<Field> avroFields = new ArrayList<>();
        for (TableFieldSchema bigQueryField : fieldSchemas) {
            avroFields.add(convertField(bigQueryField));
        }
        return Schema.createRecord(
                schemaName,
                "org.apache.beam.sdk.io.gcp.bigquery",
                "Translated Avro Schema for " + schemaName,
                false,
                avroFields);
    }

    private static Field convertField(TableFieldSchema bigQueryField) {
        Type avroType = BigQueryAvroMapper.getAvroTypes(bigQueryField.getType()).iterator().next();
        Schema elementSchema;
        if (avroType == Type.RECORD) {
            elementSchema = toGenericAvroSchema(bigQueryField.getName(), bigQueryField.getFields());
        } else {
            elementSchema = Schema.create(avroType);
        }
        Schema fieldSchema;
        if (bigQueryField.getMode() == null || "NULLABLE".equals(bigQueryField.getMode())) {
            fieldSchema = Schema.createUnion(Schema.create(Type.NULL), elementSchema);
        } else if ("REQUIRED".equals(bigQueryField.getMode())) {
            fieldSchema = elementSchema;
        } else if ("REPEATED".equals(bigQueryField.getMode())) {
            fieldSchema = Schema.createArray(elementSchema);
        } else {
            throw new IllegalArgumentException(
                    String.format("Unknown BigQuery Field Mode: %s", bigQueryField.getMode()));
        }
        return new Field(
                bigQueryField.getName(),
                fieldSchema,
                bigQueryField.getDescription(),
                (Object) null /* Cast to avoid deprecated JsonNode constructor. */);
    }

    public static TableSchema getTableSchema(Schema schema) {
        TableSchema ts = new TableSchema();
        List<TableFieldSchema> fields = getTableFieldSchema(schema);
        ts.setFields(fields);
        return ts;
    }

    public static TableSchema getOnlyNonOptionalTableSchema(Schema schema) {
        TableSchema ts = new TableSchema();
        List<TableFieldSchema> fields = getTableFieldSchema(schema, true);
        ts.setFields(fields);
        return ts;
    }

    private static List<TableFieldSchema> getTableFieldSchema(Schema schema) {
        return getTableFieldSchema(schema, false);
    }

    private static List<TableFieldSchema> getTableFieldSchema(Schema schema, boolean onlyNonOptional) {
        List<TableFieldSchema> tableFieldSchemas = new ArrayList<>();
        if (schema != null) {
            for (Schema.Field field : schema.getFields()) {
                String type = getBqType(field);
                if ("UNION".equals(type)) {
                    if (onlyNonOptional) {
                        return tableFieldSchemas;
                    } else {
                        Type insideType = field.schema().getType();
                        List<Schema> types = field.schema().getTypes();

                        if (types.size() != 2) {
                            throw new RuntimeException("Union type in Avro serializer schema must only contain two types");
                        }

                        if (types.get(0).getType().equals(Type.NULL)) {
                            insideType = types.get(1).getType();
                        } else {
                            insideType = types.get(0).getType();
                        }
                        TableFieldSchema tfs = new TableFieldSchema().setName(field.name()).setType(insideType.toString()).setMode("NULLABLE");
                        tableFieldSchemas.add(tfs);
                    }
                } else if ("ARRAY".equals(type)) {
                    Schema childSchema = field.schema().getElementType();
                    if (childSchema.getType() == Schema.Type.RECORD) {
                        List<TableFieldSchema> child = getTableFieldSchema(field.schema().getElementType());
                        TableFieldSchema tfs =
                                new TableFieldSchema()
                                        .setName(field.name())
                                        .setType("STRUCT")
                                        .setFields(child)
                                        .setMode("REPEATED");
                        tableFieldSchemas.add(tfs);
                    } else {

                        TableFieldSchema tfs =
                                new TableFieldSchema()
                                        .setName(field.name())
                                        .setType(getBqType(childSchema.getFields().get(0)))
                                        .setMode("REPEATED");
                        tableFieldSchemas.add(tfs);
                    }
                    // Create repeatable table field
                    // Handle non struct in array

                } else if ("RECORD".equals(type)) {
                    TableFieldSchema tfs =
                            new TableFieldSchema()
                                    .setName(field.name())
                                    .setType("STRUCT")
                                    .setFields(getTableFieldSchema(field.schema()));
                    tableFieldSchemas.add(tfs);
                } else if (type != null) {
                    TableFieldSchema tfs =
                            new TableFieldSchema().setName(field.name()).setType(type).setMode("REQUIRED");
                    tableFieldSchemas.add(tfs);
                }
            }
        }
        return tableFieldSchemas;
    }



    static String getBqType(Schema.Field field) {
        Schema t = field.schema();

        String logicalType = null;
        if (field.schema().getLogicalType() != null) {
            logicalType = field.schema().getLogicalType().getName();
        }

        if (t.getType().equals(Schema.Type.LONG)
                && "timestamp-millis".equals(logicalType)) {
            return "TIMESTAMP";
        } else if (t.getType().equals(Schema.Type.INT)
                && "date".equals(logicalType)) {
            return "DATE";
        } else {
            return BigQueryAvroUtils.BigQueryAvroMapper.getBigQueryTypes(t.getType());
        }
    }
}
