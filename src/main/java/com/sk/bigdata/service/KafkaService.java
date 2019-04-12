package com.sk.bigdata.service;

import com.sk.bigdata.kafka.producer.KafkaProducer;
import com.sk.bigdata.twitter.TwitterClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.List;

@Slf4j
@Service
public class KafkaService {

    private TwitterClient twitterClient;
    private KafkaProducer kafkaProducer;

    @Scheduled(fixedDelayString = "${twitter.poll.delay}")
    public void pollTwits() {
        log.info("Starting twitter poll");

        if (!twitterClient.isDone()) {
            List<String> messages = twitterClient.pollAll();

            kafkaProducer.saveAll(messages);
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
    public void setKafkaProducer(KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }
}
