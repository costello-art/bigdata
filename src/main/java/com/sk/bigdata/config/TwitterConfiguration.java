package com.sk.bigdata.config;

import com.sk.bigdata.twitter.TwitterClient;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
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
    public TwitterClient twitterMessageQueue(Authentication auth, StatusesFilterEndpoint endpoint) {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>((queueCapacityMsg));

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(new HttpHosts(Constants.STREAM_HOST))
                .authentication(auth)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        hosebirdClient.connect();

        TwitterClient twitterClient = new TwitterClient();
        twitterClient.setClient(hosebirdClient);
        twitterClient.setMsgQueue(msgQueue);

        return twitterClient;
    }
}