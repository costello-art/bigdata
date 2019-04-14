package com.sk.bigdata.config;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticSearchConfig {

    @Value("${elastic_user}")
    private String elasticUser;

    @Value("${elastic_password}")
    private String elasticPassword;

    @Value("${elastic_host}")
    private String elasticHost;

    @Bean
    public CredentialsProvider elasticCredentialsProvider() {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(elasticUser, elasticPassword));

        return credentialsProvider;
    }

    @Bean
    @Autowired
    public RestHighLevelClient elasticClient(CredentialsProvider credentialsProvider) {
        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost(elasticHost, 443, "https"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(restClientBuilder);
    }
}
