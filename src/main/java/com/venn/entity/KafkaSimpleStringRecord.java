package com.venn.entity;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;

import java.io.Serializable;

/**
 * generic string kafka record, ues by @MyKafkaRecordDeserializationSchema
 */
public class KafkaSimpleStringRecord implements Serializable {
    private static final long serialVersionUID = 4813439951036021779L;
    // kafka topic partition
    private final TopicPartition tp;
    // record kafka offset
    private final long offset;
    // record key
    private final String key;
    // record timestamp
    private final long timestamp;
    // record value
    private final String value;
    // headers
    private final Headers headers;


    public KafkaSimpleStringRecord(TopicPartition tp, long offset, String key, long timestamp, String value,Headers headers) {
        this.tp = tp;
        this.offset = offset;
        this.key = key;
        this.timestamp = timestamp;
        this.value = value;
        this.headers = headers;
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

    public Headers getHeaders() {
        return headers;
    }

    @Override
    public String toString() {
        return "MyStringKafkaRecord{" +
                "tp=topic:" + tp.topic() + ", partition: " + tp.partition() +
                ", offset=" + offset +
                ", key='" + key + '\'' +
                ", timestamp=" + timestamp +
                ", header='" + headers + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}