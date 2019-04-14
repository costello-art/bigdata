package com.sk.bigdata.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("local")
public class ElasticSearchLocalConfig {

    @Bean
    public RestHighLevelClient elasticClient() {
        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost("localhost", 9200, "http"));

        return new RestHighLevelClient(restClientBuilder);
    }
}