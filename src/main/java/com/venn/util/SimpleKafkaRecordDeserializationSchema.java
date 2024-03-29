package com.venn.util;

import com.venn.entity.KafkaSimpleStringRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;

public class SimpleKafkaRecordDeserializationSchema
        implements KafkaRecordDeserializationSchema<KafkaSimpleStringRecord> {
    private static final long serialVersionUID = -3765473065594331694L;
    private transient Deserializer<String> deserializer;

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {

    }

    @Override
    public void deserialize(
            ConsumerRecord<byte[], byte[]> record, Collector<KafkaSimpleStringRecord> collector)
            throws IOException {
        if (deserializer == null) {
            deserializer = new StringDeserializer();
        }
        long offset = record.offset();
        String key = null;
        if (record.key() != null) {
            key = new String(record.key());
        }
        long timestamp = record.timestamp();


        // makeup MyStringKafkaRecord
        KafkaSimpleStringRecord myRecord = new KafkaSimpleStringRecord(
                new TopicPartition(record.topic(), record.partition()), offset, key, timestamp, deserializer.deserialize(record.topic(), record.value()));

        collector.collect(myRecord);
    }

    @Override
    public TypeInformation<KafkaSimpleStringRecord> getProducedType() {
        return TypeInformation.of(KafkaSimpleStringRecord.class);
    }
}

