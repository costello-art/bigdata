package com.sk.bigdata.model;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TwitModel {

    @SerializedName("id_str")
    private String id;

    @SerializedName("text")
    private String text;
}