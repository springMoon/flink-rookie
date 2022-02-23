package com.venn.demo;

import com.venn.entity.KafkaSimpleStringRecord;
import com.venn.util.SimpleKafkaRecordDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class KafkaJoinRedisDemo {

    private static final String uri = "redis://localhost";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // kafka source
        KafkaSource<KafkaSimpleStringRecord> kafkaSource = KafkaSource
                .<KafkaSimpleStringRecord>builder()
                .setBootstrapServers("localhost:9092")
                .setDeserializer(new SimpleKafkaRecordDeserializationSchema())
                .setTopics("event_topic_1")
                .build();

        SingleOutputStreamOperator<String> source = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")
                .map(new MapFunction<KafkaSimpleStringRecord, String>() {
                    @Override
                    public String map(KafkaSimpleStringRecord value) throws Exception {
                        return value.getValue();
                    }
                });

        AsyncFunctionForRedis asyncFunctionForRedis = new AsyncFunctionForRedis(uri);
        //这里使用的是无序反馈结果的方法，后面两个参数是请求超时时常和时间单位，还有一个最大并发数没有设置，如果超过了最大连接数，Flink会触发反压机制来抑制上游数据的接入，保证程序正常执行
        AsyncDataStream
                .unorderedWait(source, asyncFunctionForRedis, 5L, TimeUnit.SECONDS)
                .print("match redis");


        env.execute("kafkaJoinRedis");
    }
}
