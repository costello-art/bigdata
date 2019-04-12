package com.sk.bigdata.twitter.producer;

import com.twitter.hbc.core.Client;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class TwitterClient {

    @Setter
    private Client client;

    @Setter
    private BlockingQueue<String> msgQueue;

    public boolean isDone() {
        return client.isDone();
    }

    public List<String> pollAll() {
        List<String> messages = new ArrayList<>();
        msgQueue.drainTo(messages);

        return messages;
    }

    public void done() {
        client.stop();
    }
}