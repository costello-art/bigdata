package com.sk.bigdata.model;

import com.google.gson.annotations.SerializedName;

public class TwitModel {

    @SerializedName("id_str")
    private String id;

    @SerializedName("text")
    private String text;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }
}