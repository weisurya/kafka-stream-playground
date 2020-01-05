package com.github.weisurya.kafka;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankBalanceDataProducer {
    public static void main(String[] args) {
        Properties config = new Properties();

        // Kafka bootstrap server config
        config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Producer ack config
        config.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        config.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        config.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        config.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        KafkaProducer<String, String> producer = new KafkaProducer<>(config);

        int i = 0;
        while(true) {
            System.out.println("Producing batch: " +i);
            try {
                producer.send(newRandomTransaction("surya"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("erica"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("angel"));
                Thread.sleep(100);

                i += 1;
            } catch (InterruptedException e) {
                break;
            }
        }

        producer.close();
    }

    public static ProducerRecord<String, String> newRandomTransaction(String name) {
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();

        Integer amount = ThreadLocalRandom.current().nextInt(0, 100);

        Instant now = Instant.now();

        transaction.put("name", name);
        transaction.put("amount", amount);
        transaction.put("time", now.toString());

        return new ProducerRecord<>("bank-transactions", name, transaction.toString());
    }
}
