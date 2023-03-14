package org.example;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class App {
    public static void main(final String[] args) {
        final String topic = "avro-kafka";
        final String schemaRegistry = "http://localhost:8081";
        final String kafkaServer = "localhost:9092";

        final ProducerApp producer = new ProducerApp(topic, kafkaServer, schemaRegistry);
        final ConsumerApp consumer = new ConsumerApp(topic, kafkaServer, schemaRegistry);

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        try {
            executor.scheduleAtFixedRate(producer::produce, 0, 1, TimeUnit.SECONDS);
            consumer.consume();
        } catch (final Throwable throwable) {
            throwable.printStackTrace();
        } finally {
            producer.shutDown();
        }
    }
}
