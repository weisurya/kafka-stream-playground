package com.github.weisurya.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class UserEventDataProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        Properties config = new Properties();

        // General config
        config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Ack config
        config.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        config.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        config.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");

        config.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        KafkaProducer<String, String> producer = new KafkaProducer<>(config);

        // only use .get() to ensure the writes to the topics are sequential. DO NOT DO THIS IN PRODUCTION

        System.out.println("Phase 1 - New user\n");
        producer.send(userRecord("john", "First=John,Last=Doe,Email=john.doe@gmail.com")).get();
        producer.send(purchaseRecord("john", "Apples and Bananas (1)")).get();

        Thread.sleep(10000);

        System.out.println("Phase 2 - Non-existing user\n");
        producer.send(purchaseRecord("bob", "Bob is bob")).get();

        Thread.sleep(10000);

        System.out.println("Phase 3 - Updated user information\n");
        producer.send(userRecord("john", "First=john,Last=Doe,Email=johhny.doe@gmail.com")).get();
        producer.send(purchaseRecord("john", "Oranges (3)")).get();

        Thread.sleep(10000);

        System.out.println("Phase 4 - Delete user");
        producer.send(purchaseRecord("surya", "Banana (2)")).get();
        producer.send(userRecord("surya", "First=Surya,Last=Wei,Email=wei.surya@gmail.com")).get();
        producer.send(purchaseRecord("surya", "Book (4)")).get();
        producer.send(userRecord("surya", null)).get();

        Thread.sleep(10000);

        System.out.println("Phase 5 - Deleted user");
        producer.send(userRecord("angel", "First=Angel")).get();
        producer.send(userRecord("angel", null)).get();
        producer.send(purchaseRecord("angel", "Melon (3)"));

        Thread.sleep(10000);

        System.out.println("End of demo");
        producer.close();
    }

    private static ProducerRecord<String, String> userRecord(String key, String value) {
        return new ProducerRecord<>("user-table", key, value);
    }

    private static ProducerRecord<String, String> purchaseRecord(String key, String value) {
        return new ProducerRecord<>("user-purchases", key, value);
    }
}
