package com.venn.source.mysql.cdc;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.debezium.data.Json;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class CommonKafkaSink extends RichSinkFunction<String> {

    private transient KafkaProducer<String, String> kafkaProducer;
    private transient JsonParser parser;

    @Override
    public void open(Configuration parameters) throws Exception {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("request.timeout.ms", "10");
        kafkaProducer = new KafkaProducer<>(prop);

        parser = new JsonParser();
    }

    @Override
    public void invoke(String element, Context context) throws Exception {

        JsonObject jsonObject = parser.parse(element).getAsJsonObject();
        String db = jsonObject.get("db").getAsString();
        String table = jsonObject.get("table").getAsString();
        // topic 不存在就自动创建
        String topic = db + "_" + table;

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, element);

        kafkaProducer.send(record);
        System.out.println("send message : " + element);

    }

    @Override
    public void close() throws Exception {
    }

}
