package com.sk.kafka.twitter.config;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
@Configuration
public class TwitterConfiguration {

    @Value("${twitter_consumer_key}")
    private String twConsumerKey;

    @Value("${twitter_consumer_secret}")
    private String twConsumerSecret;

    @Value("${twitter_token}")
    private String twToken;

    @Value("${twitter_secret}")
    private String twSecret;

    @Value("${queue.capacity.message}")
    private Integer queueCapacityMsg;

    @Value("${queue.capacity.event}")
    private Integer queueCapacityEvent;

    @Value("#{'${twitter.terms}'.split(',')}")
    private List<String> twTermsTrack;

    @Bean
    public Authentication hosebirdAuth() {
        return new OAuth1(twConsumerKey, twConsumerSecret, twToken, twSecret);
    }

    @Bean
    public StatusesFilterEndpoint hosebirdEndpoint() {
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(twTermsTrack);

        return hosebirdEndpoint;
    }

    @Bean
    @Autowired
    public Client clientBuilder(Authentication hosebirdAuth, StatusesFilterEndpoint hosebirdEndpoint) {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>((queueCapacityMsg));
        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>((queueCapacityEvent));

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(new HttpHosts(Constants.STREAM_HOST))
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue);

        Client hosebirdClient = builder.build();
        hosebirdClient.connect();

        return hosebirdClient;
    }
}