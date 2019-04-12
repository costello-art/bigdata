package com.sk.bigdata.kafka.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static com.sk.bigdata.config.KafkaConfiguration.TOPIC_NAME;

public class KafkaProducer {

    private Producer<String, String> producer;

    public void saveAll(List<String> messages) {
        messages.forEach(message -> producer.send(new ProducerRecord<>(TOPIC_NAME, message)));
    }

    @Autowired
    public void setProducer(Producer<String, String> producer) {
        this.producer = producer;
    }
}
