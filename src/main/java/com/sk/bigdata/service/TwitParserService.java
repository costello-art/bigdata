package com.sk.bigdata.service;

import com.google.gson.Gson;
import com.sk.bigdata.model.TwitModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TwitParserService {

    private Gson gson;

    public TwitModel parseToObj(String twitJson) {
        return gson.fromJson(twitJson, TwitModel.class);
    }

    public String parseToJson(TwitModel model) {
        return gson.toJson(model);
    }

    @Autowired
    public void setGson(Gson gson) {
        this.gson = gson;
    }
}