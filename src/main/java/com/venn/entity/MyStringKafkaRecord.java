package com.venn.entity;

import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;

public class MyStringKafkaRecord implements Serializable {
    private static final long serialVersionUID = 4813439951036021779L;

    private final TopicPartition tp;
    private final long offset;
    private final String key;
    private final long timestamp;
    private final String value;


    public MyStringKafkaRecord(TopicPartition tp, long offset, String key, long timestamp, String value) {
        this.tp = tp;
        this.offset = offset;
        this.key = key;
        this.timestamp = timestamp;
        this.value = value;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public TopicPartition getTp() {
        return tp;
    }

    public long getOffset() {
        return offset;
    }

    public String getKey() {
        return key;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "MyStringKafkaRecord{" +
                "tp=topic:" + tp.topic() + ", partition: " + tp.partition() +
                ", offset=" + offset +
                ", key='" + key + '\'' +
                ", timestamp=" + timestamp +
                ", value='" + value + '\'' +
                '}';
    }
}