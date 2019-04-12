package com.sk.bigdata.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class KafkaConfiguration {
    public static final String KAFKA_BROKERS = "localhost:9092";
    public static final Integer MESSAGE_COUNT_PER_SEC = 100000;
    public static final String CLIENT_ID = "ss_producer_1";
    public static final String TOPIC_NAME = "sk_bigdata_twitter";

    public static final String GROUP_ID_CONFIG = "consumerGroup1";
    public static final Integer MAX_NO_MESSAGE_FOUND_COUNT = 100;
    public static final String OFFSET_RESET_EARLIER = "earliest";

    public static final Integer POLL_RATE_SEC = 1;
    public static final Integer MAX_POLL_RECORDS = 10000;
    public static final Integer BATCH_SIZE_BYTES = 1024 * 1000;

    @Bean
    public Producer<String, String> producer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE_BYTES);

        return new KafkaProducer<>(props);
    }
}