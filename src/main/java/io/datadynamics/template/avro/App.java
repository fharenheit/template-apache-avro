package io.datadynamics.template.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        Schema book = SchemaBuilder.record("Book")
                .namespace("com.example.kafka")
                .fields()
                .requiredLong("id")
                .requiredLong("price")
                .requiredInt("quantity")
                .endRecord();

        Schema gift = SchemaBuilder.nullable().record("Gift")
                .namespace("com.example.kafka")
                .fields()
                .requiredLong("id")
                .requiredInt("quantity")
                .endRecord();

        Schema bookOrder = SchemaBuilder.record("BookOrder")
                .namespace("com.example.kafka")
                .fields()
                .requiredLong("orderId")
                .requiredLong("memberNo")
                .requiredString("orderDate")
                .optionalLong("couponId")
                .name("books")
                .type()
                .array()
                .items()
                .type(book)
                .noDefault()
                .name("gifts")
                .type()
                .array()
                .items()
                .type(gift)
                .noDefault()
                .endRecord();

    }
}
