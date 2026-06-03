package com.example.databenchmark.generator;

import com.example.databenchmark.schema.KpiColumn;
import com.example.databenchmark.schema.KpiSchema;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

final class KpiAvroSchemaFactory {
    private KpiAvroSchemaFactory() {}

    static Schema createSchema() {
        List<Schema.Field> fields = new ArrayList<>();
        for (KpiColumn column : KpiSchema.columns()) {
            fields.add(new Schema.Field(column.name(), avroType(column), null, (Object) null));
        }

        Schema schema = Schema.createRecord(
            "CellKpiMinute",
            "One-minute cellular KPI benchmark record.",
            "com.example.databenchmark.avro",
            false
        );
        schema.setFields(fields);
        return schema;
    }

    private static Schema avroType(KpiColumn column) {
        return switch (column.logicalType()) {
            case "timestamp_ms" -> LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
            case "string" -> Schema.create(Schema.Type.STRING);
            case "int" -> Schema.create(Schema.Type.INT);
            case "double" -> Schema.create(Schema.Type.DOUBLE);
            default -> throw new IllegalArgumentException(
                "Unsupported KPI logical type '" + column.logicalType() + "' for column '" + column.name() + "'"
            );
        };
    }
}
