package com.github.weisurya.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCountStreamApp {
    public static void main(String[] args) {
        // Reference:
        // - https://github.com/confluentinc/kafka-streams-examples/blob/5.3.2-post/src/main/java/io/confluent/examples/streams/WordCountLambdaExample.java
        // - https://docs.confluent.io/current/streams/quickstart.html

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-stream-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        // 1. Stream from Kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

        KTable<String, Long> wordCounts = wordCountInput
                                        // 2. Map values to lowercase
                                        .mapValues(value -> value.toLowerCase())
                                        // 3. Flatmap values split by space
                                        .flatMapValues(value -> Arrays.asList(value.split(" ")))
                                        // 4. Select key to apply a key (discard old key)
                                        .selectKey((key, value) -> value)
                                        // 5. Group by key before aggregation
                                        .groupByKey()
                                        // 6. Count occurences
                                        .count(Named.as("Counts"));

        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

        // Print topology
        System.out.println(streams.toString());

        // Shutdown hook to gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
