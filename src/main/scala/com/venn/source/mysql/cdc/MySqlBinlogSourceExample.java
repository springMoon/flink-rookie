package com.venn.source.mysql.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * mysql cdc demo
 */
public class MySqlBinlogSourceExample {
    public static void main(String[] args) throws Exception {

        Properties prop = new Properties();
        prop.setProperty("debezium.io.debezium.connector.mysql.SchemaChangeKey","true");
        MySqlSource<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                // 获取两个数据库的所有表
                .databaseList("venn")
                .tableList("venn.user_log_1")
                .username("root")
                .password("123456")
                // 自定义 解析器，讲数据解析成 json
                .deserializer(new CommonStringDebeziumDeserializationSchema("localhost", 3306))
                .debeziumProperties(prop)
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .fromSource(sourceFunction, WatermarkStrategy.noWatermarks(), "cdc")
                .map(str -> str)
                .print()
                // 将数据发送到不同的 topic
//                .addSink(new CommonKafkaSink())
                .setParallelism(1);

        env.execute();
    }
}
