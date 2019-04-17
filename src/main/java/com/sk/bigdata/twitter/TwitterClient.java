package com.sk.bigdata.twitter;

import com.twitter.hbc.core.Client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class TwitterClient {

    private Client client;
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

    public void setClient(Client client) {
        this.client = client;
    }

    public void setMsgQueue(BlockingQueue<String> msgQueue) {
        this.msgQueue = msgQueue;
    }
}