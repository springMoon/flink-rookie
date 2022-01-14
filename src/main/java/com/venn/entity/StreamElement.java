package com.venn.entity;

/**
 * 流实例基类
 */
public final class StreamElement<T> {

    public String source;
    public long ingestionTime;
    public String db;
    public String table;
    public T data;

    public StreamElement(T data, long ingestionTime) {
        this.data = data;
        this.ingestionTime = ingestionTime;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Long getIngestionTime() {
        return ingestionTime;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "StreamElement{" +
                "source='" + source + '\'' +
                ", ingestionTime=" + ingestionTime +
                ", db='" + db + '\'' +
                ", table='" + table + '\'' +
                ", data=" + data +
                '}';
    }
}
