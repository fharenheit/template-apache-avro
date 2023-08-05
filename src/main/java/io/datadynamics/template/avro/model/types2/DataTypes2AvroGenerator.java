package io.datadynamics.template.avro.model.types2;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

public class DataTypes2AvroGenerator {

    public static void main(String[] args) throws Exception {
        //////////////////////////
        // Serialization
        //////////////////////////

        Schema TIMESTAMP_MICROS_SCHEMA = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        TimeConversions.TimestampMicrosConversion conversion = new TimeConversions.TimestampMicrosConversion();
        long May_28_2015_21_46_53_221_843_instant = 1432849613221L * 1000 + 843;
        Instant instant = conversion.fromLong(May_28_2015_21_46_53_221_843_instant, TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMicros());

        DataTypes2 t1 = DataTypes2.newBuilder()
                .setTypeBoolean(false)
                .setTypeInt(1)
                .setTypeFloat(123123f)
                .setTypeDouble(3.14)
                .setTypeLong(123123L)
                .setTypeDate(LocalDate.now())
                .setTypeString("Hello World")
                .setTypeTimeInMicros(LocalTime.now())
                .setTypeTimeInMillis(LocalTime.now())
                .setTypeTimestampInMillis(Instant.now())
                .setTypeStringTimestampInMillis("2022-11-11 11:11:11.111")
                .setTypeBytesDecimal(new BigDecimal("11.11"))
                .build();

        DatumWriter<DataTypes2> datumWriter = new SpecificDatumWriter<>(DataTypes2.class);
        DataFileWriter<DataTypes2> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(t1.getSchema(), new File("datatypes2.avro"));
        dataFileWriter.append(t1);
        dataFileWriter.append(t1);
        dataFileWriter.append(t1);
        dataFileWriter.append(t1);
        dataFileWriter.append(t1);
        dataFileWriter.append(t1);
        dataFileWriter.close();

        //////////////////////////
        // Deserialization
        //////////////////////////

        DatumReader<DataTypes2> datumReader = new SpecificDatumReader<>(DataTypes2.class);
        DataFileReader<DataTypes2> dataFileReader = new DataFileReader<>(new File("datatypes2.avro"), datumReader);
        DataTypes2 type = null;
        while (dataFileReader.hasNext()) {
            type = dataFileReader.next(type);
            System.out.println(formatJSONStr(type.toString(), 4));

            /*
                {
                    "TypeBoolean": false,
                    "TypeInt": 1,
                    "TypeLong": 123123,
                    "TypeFloat": 123123.0,
                    "TypeDouble": 3.14,
                    "TypeString": "Hello World",
                    "TypeBytesDecimal": 11.11,
                    "TypeDate": "2023-08-06",
                    "TypeTimeInMillis": "08:45:12.545",
                    "TypeTimeInMicros": "08:45:12.545589",
                    "TypeTimestampInMillis": "2023-08-05T23:45:12.545Z",
                    "TypeStringTimestampInMillis": "2022-11-11 11:11:11.111"
                }
             */
        }
    }

    public static String formatJSONStr(final String json_str, final int indent_width) {
        final char[] chars = json_str.toCharArray();
        final String newline = System.lineSeparator();

        String ret = "";
        boolean begin_quotes = false;

        for (int i = 0, indent = 0; i < chars.length; i++) {
            char c = chars[i];

            if (c == '\"') {
                ret += c;
                begin_quotes = !begin_quotes;
                continue;
            }

            if (!begin_quotes) {
                switch (c) {
                    case '{':
                    case '[':
                        ret += c + newline + String.format("%" + (indent += indent_width) + "s", "");
                        continue;
                    case '}':
                    case ']':
                        ret += newline + ((indent -= indent_width) > 0 ? String.format("%" + indent + "s", "") : "") + c;
                        continue;
                    case ':':
                        ret += c + " ";
                        continue;
                    case ',':
                        ret += c + newline + (indent > 0 ? String.format("%" + indent + "s", "") : "");
                        continue;
                    default:
                        if (Character.isWhitespace(c)) continue;
                }
            }

            ret += c + (c == '\\' ? "" + chars[++i] : "");
        }

        return ret;
    }

/*

    public static ByteBuffer _toByteBuffer(double value) {
        return new Conversions.DecimalConversion().toBytes(
                new BigDecimal(value, MathContext.DECIMAL32).setScale(2, BigDecimal.ROUND_HALF_EVEN),
                LogicalTypes.decimal(6, 2).addToSchema(Schema.create(Schema.Type.BYTES)),
                LogicalTypes.decimal(6, 2)
        );
    }

    public static ByteBuffer toByteBuffer(double value) {
        LogicalType type = LogicalTypes.decimal(6, 2);
        BigDecimal bigDecimal = new BigDecimal(value, MathContext.DECIMAL32).setScale(((LogicalTypes.Decimal) type).getScale(), BigDecimal.ROUND_HALF_EVEN);
        return ByteBuffer.wrap(bigDecimal.unscaledValue().toByteArray());
    }
*/
}
