package um.si;


import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import sun.net.www.content.text.Generic;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class OrderConsumer {

    private final static String TOPIC = "kafka-orders";

    private static KafkaConsumer<String, GenericRecord> createConsumer() {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "AvroConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-orders");
        // Configure the KafkaAvroDeserializer.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        // Schema Registry location.
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer(props);
    }

    public static void main(String[] args) throws Exception {
        /*Schema schema = SchemaBuilder.record("Order")
                .fields()
                .requiredString("order_no")
                .requiredLong("date")
                .requiredInt("rest_id")
                .requiredInt("user_id")
                .requiredInt("courier_id")
                .requiredInt("item_id")
                .requiredInt("quantity")
                .endRecord();*/

        KafkaConsumer consumer = createConsumer()   ;
        consumer.subscribe(Arrays.asList(TOPIC));
        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(2000);
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
