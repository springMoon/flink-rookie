package com.venn.demo;

import com.venn.entity.KafkaSimpleStringRecord;
import com.venn.util.SimpleKafkaRecordDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class KafkaJoinRedisDemo {

    private static final String uri = "redis://localhost";
    private static final String bootstrapServer = "localhost:9092";
    private static final String topic = "user_log";

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


        // get value
        SingleOutputStreamOperator<String> source = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")
                .map((MapFunction<KafkaSimpleStringRecord, String>) value -> value.getValue());

        // async redis
        AsyncRedisFunction asyncRedisFunction = new AsyncRedisFunction(uri);
        SingleOutputStreamOperator<String> asyncStream = AsyncDataStream
                .unorderedWait(source, asyncRedisFunction, 5L, TimeUnit.SECONDS);

        // print result
        asyncStream
                .print("match redis");

        env.execute("kafkaJoinRedis");
    }
}
