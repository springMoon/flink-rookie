package com.venn.entity;

/**
 * 点击流实体对象
 */
public class Behavior {
    private String userId;
    private String url;
    private long ts;

    public Behavior(String userId, String url, long ts) {
        this.userId = userId;
        this.url = url;
        this.ts = ts;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "Behavior{" +
                "userId='" + userId + '\'' +
                ", url='" + url + '\'' +
                ", ts=" + ts +
                '}';
    }
}

