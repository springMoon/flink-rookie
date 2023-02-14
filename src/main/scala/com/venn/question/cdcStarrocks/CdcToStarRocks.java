package com.venn.question.cdcStarrocks;

import com.venn.source.mysql.cdc.CommonStringDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * mysql cdc demo
 * <p>
 * cdc 整库同步数据到 starrocks
 * <p>
 * 局限：
 * 1. 还未实现 starrocks 端表结构跟随 源端表结构同步变更
 * 2. 为了保证效率，仅会在每一个表第一次来的时候判断目标段是否存在该表，如果已经判定该表不存在，后续直接忽略该表的数据变更
 * 3. 部分不导入的表，只在sink 的时候做了过滤，前面的操作还是要继续，可以考虑在 反序列化活map中过滤掉目标库中不存在的表数据
 */
public class CdcToStarRocks {

    // 每个批次最大条数和等待时间
    private static int batchSize = 10000;
    private static long batchInterval = 10 *1000;

    public static void main(String[] args) throws Exception {

        String ip = "localhost";
        int port = 3306;
        String db = "venn";
//        String table = "venn.user_log,venn.user_log_1";
        String table = "venn.*";
        String user = "root";
        String pass = "123456";

        String starrocksIp = "10.201.0.230";
        String starrocksPort = "29030";
        String starrocksLoadPort = "28030";
        String starrocksUser = "root";
        String starrocksPass = "123456";
        String starrocksDb = "test";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MySqlSource<String> sourceFunction = MySqlSource.<String>builder()
                .hostname(ip)
                .port(port)
                // 获取两个数据库的所有表
                .databaseList(db)
                .tableList(table)
                .username(user)
                .password(pass)
                .startupOptions(StartupOptions.latest())
//                .startupOptions(StartupOptions.initial())
                // do not cache schema change
//                .includeSchemaChanges(true)
                // 自定义 解析器，讲数据解析成 json
                .deserializer(new CommonStringDebeziumDeserializationSchema(ip, port))
                .build();

        env
                .fromSource(sourceFunction, WatermarkStrategy.noWatermarks(), "cdc")
                .name("source")
                .uid("source")
//                 json 字符串转 CdcRecord
                .map(new CdcStarMapFunction())
                .name("map")
                .keyBy(  record -> record.getDb() + "_" + record.getTable())
                .process(new CdcStarProcessFunction(batchSize, batchInterval))
                .name("process")
                .uid("process")
                .addSink(new StarRocksSink(starrocksIp, starrocksPort, starrocksLoadPort, starrocksUser, starrocksPass, starrocksDb))
                .name("sink");

        env.execute("cdcToStarRocks");
    }
}
