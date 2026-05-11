package com.venn.demo;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.venn.entity.KafkaSimpleStringRecord;
import com.venn.util.SimpleKafkaRecordDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class KafkaDemo {

    private static final String bootstrapServer = "10.201.0.191:9092";
    private static final String topic = "user_behavior_debezium_json";

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
                        .setTopic("user_behavior_debezium_json_jar_out")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();


        // get value
        SingleOutputStreamOperator<String> source = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")
                .flatMap(new RichFlatMapFunction<KafkaSimpleStringRecord, String>() {

                             @Override
                             public void flatMap(KafkaSimpleStringRecord kafkaSimpleStringRecord, Collector<String> collector) throws Exception {


                                 JsonObject json = JsonParser.parseString(kafkaSimpleStringRecord.getValue()).getAsJsonObject();
                                 if (json.has("after")) {
                                     String after = json.getAsJsonObject("after").toString();
                                     collector.collect(after);
                                 }
                             }
                         }
                );

        // print result
        source
                .disableChaining()
                .sinkTo(sink)
        ;

        env.execute("kafkaDemo");
    }
}
