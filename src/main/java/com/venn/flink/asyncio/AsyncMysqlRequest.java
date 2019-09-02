package com.venn.flink.asyncio;

import com.alibaba.fastjson.JSON;
import com.venn.common.Common;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.concurrent.TimeUnit;


public class AsyncMysqlRequest {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        FlinkKafkaConsumer<ObjectNode> source = new FlinkKafkaConsumer<>("async", new JsonNodeDeserializationSchema(), Common.getProp());
        source.setStartFromLatest();

        // 接收kafka数据，转为User 对象
        DataStream<AsyncUser> input = env.addSource(source)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ObjectNode>(Time.seconds(60)) {
                    @Override
                    public long extractTimestamp(ObjectNode element) {
                        return element.get("id").asLong(0) + 1000;
                    }
                })
                .map(value -> {
            String id = value.get("id").asText();
            String username = value.get("username").asText();
            String password = value.get("password").asText();

            return new AsyncUser(id, username, password);
        });
        // 异步IO 获取mysql数据, timeout 时间 1s，容量 100（超过100个请求，会反压上游节点）
        DataStream async = AsyncDataStream
                .unorderedWait(input,
                        new AsyncFunctionForMysqlJava(),
                        1000,
                        TimeUnit.MILLISECONDS,
                        10);

        async.map(user -> {
            return JSON.toJSON(user).toString();
        })
                .print();

        env.execute("asyncForMysql");

    }
}
