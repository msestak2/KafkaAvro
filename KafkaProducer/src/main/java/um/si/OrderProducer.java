package um.si;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import sun.net.www.content.text.Generic;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class OrderProducer {

    private final static String TOPIC = "kafka-orders";

    private static KafkaProducer createProducer() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // Configure the KafkaAvroSerializer.
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        // Schema Registry location.
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        return new KafkaProducer(props);
    }

    private static ProducerRecord<Object,Object> generateRecord(Schema schema) {
        Random rand = new Random();
        GenericRecord avroRecord = new GenericData.Record(schema);

        String orderNo = String.valueOf((int)(Math.random()*(10000 - 0 + 1) + 1));
        avroRecord.put("order_no",orderNo);
        avroRecord.put("date",System.currentTimeMillis());
        avroRecord.put("rest_id",Integer.valueOf(rand.nextInt((9-1)) + 1));
        avroRecord.put("user_id",Integer.valueOf(rand.nextInt((9-1)) + 1));
        avroRecord.put("courier_id",Integer.valueOf(rand.nextInt((9-1)) + 1));
        avroRecord.put("item_id",Integer.valueOf(rand.nextInt((100-1)) + 1));
        avroRecord.put("quantity",Integer.valueOf(rand.nextInt((8-1)) + 1));
       /* Order order = new Order(orderNo,System.currentTimeMillis(), Integer.valueOf(rand.nextInt((9-0) + 1)),
                Integer.valueOf(rand.nextInt((9-0) + 1)), Integer.valueOf(rand.nextInt((9-0) + 1))
                new HashMap<String, Integer>(){{
                    put("item_id", Integer.valueOf(rand.nextInt((9-0) + 1)));
                    put("quantity", Integer.valueOf(rand.nextInt((8-0) + 1)));
                }}
        );*/
        ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(TOPIC, orderNo, avroRecord);
        return producerRecord;
    }


    public static void main(String[] args) throws Exception {
        Schema schema = SchemaBuilder.record("Order")
                .fields()
                .requiredString("order_no")
                .requiredLong("date")
                .requiredInt("rest_id")
                .requiredInt("user_id")
                .requiredInt("courier_id")
                .requiredInt("item_id")
                .requiredInt("quantity")
                .endRecord();

       KafkaProducer producer = createProducer();

        while(true){
            producer.send(generateRecord(schema));
            System.out.println("[RECORD] Sent new order object.");
            Thread.sleep(10000);
        }
    }
}
