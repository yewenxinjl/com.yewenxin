package com.yewenxin.pojo;

import java.sql.Timestamp;

public class UrlViewCount {
    public String url;
    public Long count;
    public Long startWindowTime;
    public Long endWindowTime;

    public UrlViewCount() {
    }

    public UrlViewCount(String url, Long count, Long startWindowTime, Long endWindowTime) {
        this.url = url;
        this.count = count;
        this.startWindowTime = startWindowTime;
        this.endWindowTime = endWindowTime;
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", startWindowTime=" + new Timestamp(startWindowTime) +
                ", endWindowTime=" + new Timestamp(endWindowTime) +
                '}';
    }
}
