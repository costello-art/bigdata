package com.sk.bigdata.service;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.IOException;

@Service
@Slf4j
public class ElasticService {

    private RestHighLevelClient elasticClient;

    public String write(String message, XContentType contentType) throws IOException {
        IndexRequest request = new IndexRequest("twitter", "tweets")
                .source(message, contentType);

        IndexResponse indexResponse = elasticClient.index(request, RequestOptions.DEFAULT);

        return indexResponse.getId();
    }

    @PreDestroy
    public void closeElasticClient() {
        log.info("Closing ElasticClient");
        try {
            elasticClient.close();
        } catch (IOException e) {
            log.error("Unable to close ElasticClient", e);
        }
    }

    @Autowired
    public void setElasticClient(RestHighLevelClient elasticClient) {
        this.elasticClient = elasticClient;
    }
}