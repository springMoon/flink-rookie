package com.venn.demo;

import com.venn.common.Common;
import com.venn.util.DateTimeUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class TypeTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        env.setParallelism(1);
        String bootstrapServer = "localhost:9092";
        String topic = "user_log";
        // source
        KafkaSource kafkaSource = KafkaSource
                .builder()
                .setBootstrapServers(bootstrapServer)
                .setGroupId("ra")
                .setTopics(topic)
                .setBounded(OffsetsInitializer.timestamp(DateTimeUtil.parse("2022-04-29 12:00:00").getTime()))
//                .setUnbounded(OffsetsInitializer.latest())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new KafkaRecordDeserializationSchema() {
                    @Override
                    public TypeInformation getProducedType() {
                        return null;
                    }
                    @Override
                    public void deserialize(ConsumerRecord record, Collector out) throws IOException {
                        byte[] value = (byte[])record.value();

                        out.collect(new String(value));
                    }
                })
                .build();


        SingleOutputStreamOperator source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")
                .returns(String.class);

        SingleOutputStreamOperator stream = source.map(aa -> aa)
                .returns(String.class)
                .map(aa -> 1)
                .returns(Integer.class)
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(0)
                .map(aa -> "" + aa)
                .returns(String.class);

        KafkaSink sink = KafkaSink
                .<String>builder()
                .setBootstrapServers(bootstrapServer)
                .setKafkaProducerConfig(Common.getProp())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic + "_sink")
                        .setKeySerializationSchema(new SimpleStringSchema())
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setTransactionalIdPrefix("xxx" + System.currentTimeMillis())
                .build();

        stream.sinkTo(sink);


        env.execute("typeTest");
    }
}
