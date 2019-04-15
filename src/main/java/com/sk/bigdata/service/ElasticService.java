package com.sk.bigdata.service;

import com.sk.bigdata.model.TwitModel;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.List;

@Service
@Slf4j
public class ElasticService {

    private RestHighLevelClient elasticClient;
    private TwitParserService twitParserService;

    public String write(String message, XContentType contentType) throws IOException {
        return write(null, message, contentType);
    }

    public String write(String id, String message, XContentType contentType) throws IOException {
        IndexRequest request = new IndexRequest("twitter", "tweets", id)
                .source(message, contentType);

        IndexResponse indexResponse = elasticClient.index(request, RequestOptions.DEFAULT);

        return indexResponse.getId();
    }

    public void writeAll(List<TwitModel> twitModels) throws IOException {
        if (CollectionUtils.isEmpty(twitModels)) {
            log.debug("No data to store to ElasticSearch, skipping.");
            return;
        }

        log.debug("Storing {} item(s) into ElasticSearch", twitModels.size());
        BulkRequest bulkRequest = new BulkRequest();

        twitModels.forEach(m -> {
            IndexRequest indexRequest = new IndexRequest("twitter", "tweets", m.getId())
                    .source(twitParserService.parseToJson(m), XContentType.JSON);

            bulkRequest.add(indexRequest);
        });

        BulkResponse bulkResponse = elasticClient.bulk(bulkRequest, RequestOptions.DEFAULT);

        log.debug("Bulk request done, status: {}", bulkResponse.status().toString());
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

    @Autowired
    public void setTwitParserService(TwitParserService twitParserService) {
        this.twitParserService = twitParserService;
    }
}