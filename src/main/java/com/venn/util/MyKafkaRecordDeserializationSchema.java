package com.venn.util;

import com.google.gson.Gson;
import com.venn.entity.MyStringKafkaRecord;
import com.venn.entity.UserLog;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;

public class MyKafkaRecordDeserializationSchema
        implements KafkaRecordDeserializationSchema<MyStringKafkaRecord> {
    private static final long serialVersionUID = -3765473065594331694L;
    private transient Deserializer<String> deserializer;

    @Override
    public void deserialize(
            ConsumerRecord<byte[], byte[]> record, Collector<MyStringKafkaRecord> collector)
            throws IOException {
        if (deserializer == null) {
            deserializer = new StringDeserializer();
        }
        long offset = record.offset();
        String key = new String(record.key());
        long timestamp = record.timestamp();


        // makeup MyStringKafkaRecord
        MyStringKafkaRecord myRecord = new MyStringKafkaRecord(
                new TopicPartition(record.topic(), record.partition()), offset, key, timestamp, deserializer.deserialize(record.topic(), record.value()));

        collector.collect(myRecord);
    }

    @Override
    public TypeInformation<MyStringKafkaRecord> getProducedType() {
        return TypeInformation.of(MyStringKafkaRecord.class);
    }
}

