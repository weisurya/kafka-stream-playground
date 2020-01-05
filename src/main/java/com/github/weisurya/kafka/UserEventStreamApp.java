package com.github.weisurya.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class UserEventStreamApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-event-enricher-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, String> userGlobalTable = builder.globalTable("user-table");

        KStream<String, String> userPurchases = builder.stream("user-purchase");

        KStream<String, String> userPurchasesEnrichedInnerJoin = userPurchases.join(
                userGlobalTable,
                (key, value) -> key,
                (userPurchase, userInfo) -> "Purchase=" + userPurchase + ",UserInfo=[" + userInfo + "]"
        );

        userPurchasesEnrichedInnerJoin.to("user-purchase-enriched-inner-join");

        KStream<String, String> userPurchaseEnrichedLeftJoin = userPurchases.join(
                userGlobalTable,
                (key, value) -> key,
                (userPurchase, userInfo) -> {
                    if (userInfo != null) {
                        return "Purchase=" + userPurchase + ",UserInfo=[" + userInfo + "]";
                    } else {
                        return "Purchase=" + userPurchase + ".UserInfo=null";
                    }
                }
        );

        userPurchaseEnrichedLeftJoin.to("user-purchase-enriched-left-join");

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

        // print the topology
        streams.localThreadsMetadata().forEach(data -> System.out.println(data));

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
