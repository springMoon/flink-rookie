package com.venn.connector.starrocks;

/**
 * @Classname DdlMysqlToStarRocks
 * @Description 使用 mysql ddl 创建 StarRocks 表, 支持 mysql information 和 desc 读表结构
 * @Date 2024/3/8
 * @Created by venn
 */
public class DdlMysqlToStarRocks {


    public static void main(String[] args) {

        String sourceIp = "rm-2ze82f881xft670dm.mysql.rds.aliyuncs.com";
        int sourcePort = 3306;
        String sourceUser = "deepexi";
        String sourcePass = "mvqRxerKIgQadEO1M74";
        String sourceDb = "showyu-digital-operation";
        String sourceTable = "task_standard_temp";

        String sinkId = "10.20.131.192";
        int sinkPort = 9030;
        String sinkUser = "root";
        String sinkPass = "showyu123";
        String sinkDb = "test";
        String sinkTable = "task_standard_temp";



    }

}
