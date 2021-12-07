package um.si;


import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.*;
public class OrderStreamer {
    private static Properties setupApp() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "GroupOrders");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class.getName());
        props.put("schema.registry.url", "http://0.0.0.0:8081");
        props.put("input.topic.name", "kafka-orders");
        props.put("default.deserialization.exception.handler", "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");

        return props;
    }

    public static void main(String[] args) throws Exception {

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                "http://0.0.0.0:8081");

        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, GenericRecord> inputStream = builder.stream("kafka-orders", Consumed.with(Serdes.Integer(), valueGenericAvroSerde));

        inputStream.map((k,v)->new KeyValue<>(Integer.valueOf(v.get("user_id").toString()),v.get("order_no").toString()))
                .groupByKey(Grouped.with(Serdes.Integer(), Serdes.String())).count().toStream().mapValues(value -> value.toString())
                .to("kafka-grouped-orders", Produced.with(Serdes.Integer(), Serdes.String()));

        inputStream.print(Printed.toSysOut());

        KTable<Integer, String> orderCountsStream = builder.table("kafka-grouped-orders", Consumed.with(Serdes.Integer(), Serdes.String()));
        orderCountsStream.filter((key, value) -> Integer.valueOf(value) > 5).toStream()
                .to("kafka-filter-5-orders", Produced.with(Serdes.Integer(), Serdes.String()));

        inputStream
                .map((k,v)->new KeyValue<>(v.get("order_no").toString(),Integer.valueOf(v.get("quantity").toString())))
                .groupByKey(Grouped.with(Serdes.String(),Serdes.Integer()))
                .reduce(Integer::sum)
                .toStream()
                .to("kafka-order-quantities", Produced.with(Serdes.String(), Serdes.Integer()));


        Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, setupApp());
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
