package com.venn.source.mysql.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;

/**
 * mysql cdc demo
 */
public class MySqlBinlogSourceExample {
    public static void main(String[] args) throws Exception {

        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("venn") // monitor all tables under inventory database
                .username("root")
                .password("123456")
                // 自定义 解析器，讲数据解析成 json
                .deserializer(new MyStringDebeziumDeserializationSchema("localhost", 3306))
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .addSource(sourceFunction)
                .map(str -> str)
                // 讲数据发送到不同的 topic
                .addSink(new CommonKafkaSink())
                .setParallelism(1);

        env.execute();
    }
}
