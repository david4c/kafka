package org.example;

import com.epam.avro.Greeting;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerApp {

    private final String topic;
    private final KafkaProducer<String, Greeting> kafkaProducer;

    public ProducerApp(final String topic, String kafkaServer, String schemaRegistry) {
        this.topic = topic;
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class
        );
        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class
        );
        props.put("schema.registry.url", schemaRegistry);
        kafkaProducer = new KafkaProducer<>(props);
    }

    public void produce() {
        Greeting greeting = Greeting
                .newBuilder()
                .setGreet("Hello")
                .setTime(System.currentTimeMillis())
                .build();

        ProducerRecord<String, Greeting> record = new ProducerRecord<>(topic, "key", greeting);
        try {
            kafkaProducer.send(record);
        } catch (SecurityException e) {
            e.printStackTrace();
        }
    }

    public void shutDown() {
        kafkaProducer.flush();
        kafkaProducer.close();
    }

}
