package com.venn.demo;

import com.venn.entity.KafkaSimpleStringRecord;
import com.venn.util.SimpleKafkaRecordDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaJoinRedisDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String srcBootstrapServers = "localhost:9092";
        String srcGroupId = "group123";
        String Topic1 = "event_topic_1";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", srcBootstrapServers);
        properties.setProperty("group.id", srcGroupId);

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

//        source.print("kafka数据");

        String hostAndPort = "localhost:6379";
        List<String> list = Arrays.asList(hostAndPort.split(","));
//        RedisSide redisSide = new RedisSide(list);
        //这里使用的是无序反馈结果的方法，后面两个参数是请求超时时常和时间单位，还有一个最大并发数没有设置，如果超过了最大连接数，Flink会触发反压机制来抑制上游数据的接入，保证程序正常执行
//        AsyncDataStream.unorderedWait(source, redisSide, 5L, TimeUnit.SECONDS).print("与redis匹配的结果");

        AsyncRedis asyncRedis = new AsyncRedis(list);
        //这里使用的是无序反馈结果的方法，后面两个参数是请求超时时常和时间单位，还有一个最大并发数没有设置，如果超过了最大连接数，Flink会触发反压机制来抑制上游数据的接入，保证程序正常执行
        AsyncDataStream
                .unorderedWait(source, asyncRedis, 5L, TimeUnit.SECONDS)
                .print("match redis");


        env.execute("kafkaJoinRedis");
    }
}
