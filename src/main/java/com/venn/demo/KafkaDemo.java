package com.venn.demo;

import com.venn.entity.KafkaSimpleStringRecord;
import com.venn.util.SimpleKafkaRecordDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class KafkaDemo {

    private static final String uri = "redis://localhost";
    private static final String bootstrapServer = "10.201.0.191:9092";
    private static final String topic = "employee";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // kafka source
        KafkaSource<KafkaSimpleStringRecord> kafkaSource = KafkaSource
                .<KafkaSimpleStringRecord>builder()
                .setBootstrapServers(bootstrapServer)
                .setDeserializer(new SimpleKafkaRecordDeserializationSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setTopics(topic)
                .build();


        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServer)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("employee_2")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();


        // get value
        SingleOutputStreamOperator<String> source = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")
                .map((MapFunction<KafkaSimpleStringRecord, String>) value -> value.getValue());

        // print result
        source
                .disableChaining()
                .sinkTo(sink)
        ;

        env.execute("kafkaDemo");
    }
}
