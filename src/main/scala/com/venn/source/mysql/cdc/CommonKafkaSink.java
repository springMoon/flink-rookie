package com.venn.source.mysql.cdc;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class CommonKafkaSink extends RichSinkFunction<String> {

    protected static final Logger LOG = LoggerFactory.getLogger(CommonKafkaSink.class);
    private transient KafkaProducer<String, String> kafkaProducer;
    private transient JsonParser parser;

    @Override
    public void open(Configuration parameters) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("request.timeout.ms", "10");
        kafkaProducer = new KafkaProducer<>(prop);
        parser = new JsonParser();

    }

    @Override
    public void invoke(String element, Context context) {

        JsonObject jsonObject = parser.parse(element).getAsJsonObject();
        String db = jsonObject.get("db").getAsString();
        String table = jsonObject.get("table").getAsString();
        // topic 不存在就自动创建
        String topic = db + "_" + table;
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, element);
        kafkaProducer.send(record);
    }

    @Override
    public void close() {
        kafkaProducer.close();
    }

}
