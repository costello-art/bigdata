package com.sk.bigdata.service;

import com.sk.bigdata.model.TwitModel;
import com.sk.bigdata.twitter.TwitterClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.sk.bigdata.config.KafkaConfiguration.TOPIC_NAME;

@Service
public class KafkaService {
    private Logger log = LoggerFactory.getLogger(this.getClass());

    private TwitterClient twitterClient;
    private Producer<String, String> kafkaProducer;
    private Consumer<String, String> kafkaConsumer;
    private ElasticService elasticService;
    private TwitParserService twitParserService;

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

        List<TwitModel> twitModelList = getTwitModels(consumerRecords);

        try {
            elasticService.writeAll(twitModelList);
        } catch (IOException e) {
            log.error("Unable to store message to ElasticSearch", e);
        }
    }

    private List<TwitModel> getTwitModels(ConsumerRecords<String, String> consumerRecords) {
        List<TwitModel> twitModelList = new ArrayList<>();
        consumerRecords.forEach(tweet -> twitModelList.add(twitParserService.parseToObj(tweet.value())));

        return twitModelList;
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

    @Autowired
    public void setTwitParserService(TwitParserService twitParserService) {
        this.twitParserService = twitParserService;
    }
}