package com.venn.source.mysql.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * mysql cdc demo
 */
public class MySqlBinlogSourceExample {
    public static void main(String[] args) throws Exception {

        String ip = "10.201.0.166";
        int port = 3306;
        String dbReg = "deepexi.*";
        String tableReg = "[deepexi|dolphinscheduler].*";
        String user = "root";
        String pass = "daas2020";
        String bootstrapServer = "dcmp12:9092";

        if (args.length > 6) {
            ip = args[0];
            port = Integer.parseInt(args[1]);
            dbReg = args[2];
//            tableReg = args[3];
            user = args[4];
            pass = args[5];
        }


        Properties prop = new Properties();
        MySqlSource<String> sourceFunction = MySqlSource.<String>builder()
                .hostname(ip)
                .port(port)
                // 获取两个数据库的所有表
                .databaseList(dbReg)
                .tableList(tableReg)
                .username(user)
                .password(pass)
                .startupOptions(StartupOptions.latest())
                // 自定义 解析器，讲数据解析成 json
                .deserializer(new CommonStringDebeziumDeserializationSchema(ip, port))
                .debeziumProperties(prop)
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .fromSource(sourceFunction, WatermarkStrategy.noWatermarks(), "cdc")
                .map(str -> str)
//                .print()
                // 将数据发送到不同的 topic
                .addSink(new CommonKafkaSink(bootstrapServer))
                .setParallelism(1);

        env.execute();
    }
}
