package io.datadynamics.template.avro;

import org.apache.avro.Schema;

public class Parsing {

    public static void main(String[] args) {
        String avroSchemaJson = "{\n" +
                "  \"namespace\": \"io.datadynamics.template.avro.model.types2\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"DataTypes2\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"TypeBoolean\",\n" +
                "      \"type\": [\"null\", \"boolean\"]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeInt\",\n" +
                "      \"type\": [\"null\", \"int\"]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeLong\",\n" +
                "      \"type\": [\"null\", \"long\"]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeFloat\",\n" +
                "      \"type\": [\"null\", \"float\"]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeDouble\",\n" +
                "      \"type\": [\"null\", \"double\"]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeString\",\n" +
                "      \"type\": [\"null\", \"string\"]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeBytesDecimal\",\n" +
                "      \"type\": [\"null\", {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 6, \"scale\": 2}]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeDate\",\n" +
                "      \"type\": [\"null\", {\"type\": \"int\", \"logicalType\": \"date\"}]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeTimeInMillis\",\n" +
                "      \"type\": [\"null\", {\"type\": \"int\", \"logicalType\": \"time-millis\"}]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeTimeInMicros\",\n" +
                "      \"type\": [\"null\", {\"type\": \"long\", \"logicalType\": \"time-micros\"}]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeTimestampInMillis\",\n" +
                "      \"type\": [\"null\", {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TypeStringTimestampInMillis\",\n" +
                "      \"type\": [\"null\", {\"type\": \"string\", \"logicalType\": \"timestamp-millis\"}]\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(avroSchemaJson);
        for (Schema.Field field : schema.getFields()) {
            System.out.println(field);
            System.out.println(field.schema());
        }
    }

}