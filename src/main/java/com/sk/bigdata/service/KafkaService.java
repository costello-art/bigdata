package com.sk.bigdata.service;

import com.sk.bigdata.twitter.TwitterClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static com.sk.bigdata.config.KafkaConfiguration.TOPIC_NAME;

@Slf4j
@Service
public class KafkaService {

    private TwitterClient twitterClient;
    private Producer<String, String> kafkaProducer;
    private Consumer<String, String> kafkaConsumer;
    private ElasticService elasticService;

    @Scheduled(fixedDelayString = "${twitter.poll.delay}")
    public void storeTwitsToKafka() {
        log.debug("Starting polling Twitter for twits");

        if (!twitterClient.isDone()) {
            List<String> messages = twitterClient.pollAll();

            log.debug("Got {} message(s)", messages.size());

            messages.forEach(message -> kafkaProducer.send(new ProducerRecord<>(TOPIC_NAME, message)));
        } else {
            log.debug("No new twits from Twitter");
        }
    }

    @Scheduled(fixedDelayString = "${kafka.consumer.poll.delay}")
    public void storeTwitsToElastic() {
        log.debug("Starting polling messages from Kafka");
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

        log.debug("Got {} message(s) form Kafka", consumerRecords.count());

        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            String value = consumerRecord.value();

            try {
                elasticService.write(value, XContentType.JSON);
            } catch (IOException e) {
                log.error("Unable to store message to ElasticSearch", e);
            }
        }
    }

    @PreDestroy
    public void stopClient() {
        log.info("Stopping twitter client");
        twitterClient.done();
    }

    @Autowired
    public void setTwitterClient(TwitterClient twitterClient) {
        this.twitterClient = twitterClient;
    }

    @Autowired
    public void setKafkaProducer(Producer<String, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @Autowired
    public void setKafkaConsumer(Consumer<String, String> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    @Autowired
    public void setElasticService(ElasticService elasticService) {
        this.elasticService = elasticService;
    }
}