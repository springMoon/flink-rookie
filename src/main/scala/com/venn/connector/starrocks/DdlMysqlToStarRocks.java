package com.venn.connector.starrocks;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * @Classname DdlMysqlToStarRocks
 * @Description 使用 mysql ddl 创建 StarRocks 表, 支持 mysql information 和 desc 读表结构
 * @Date 2024/3/8
 * @Created by venn
 */
public class DdlMysqlToStarRocks {

    private static Logger LOG = LoggerFactory.getLogger(DdlMysqlToStarRocks.class);

    private static String sql = "select t1.TABLE_NAME, ifnull(t1.TABLE_COMMENT, ''), t2.COLUMN_NAME, t2.COLUMN_TYPE, ifnull(t2.COLUMN_COMMENT, '')\n" +
            "from information_schema.TABLES t1\n" +
            "    left join information_schema.COLUMNS t2 on t1.TABLE_CATALOG = t2.TABLE_CATALOG\n" +
            "        and t1.TABLE_SCHEMA = t2.TABLE_SCHEMA and t1.TABLE_NAME = t2.TABLE_NAME\n" +
            "where t1.TABLE_SCHEMA = ?" +
            "and t2.TABLE_NAME = ?";

    public static void main(String[] args) throws SQLException {

        String targetTableType = "PRIMARY KEY";

        String sourceIp = "localhost";
        int sourcePort = 3306;
        String sourceUser = "root";
        String sourcePass = "123456";
        String sourceDb = "member-center";
        String sourceTable = "member";
        String keyIndex = "1";

        String sinkIp = "localhost";
        int sinkPort = 9030;
        String sinkUser = "root";
        String sinkPass = "123456";
        String sinkDb = "test";
        String sinkTable = "member";

        String sinkUrl = "jdbc:mysql://" + sinkIp + ":" + sinkPort;


        initTable(targetTableType, sourceIp, sourcePort, sourceUser, sourcePass, sourceDb, sourceTable, keyIndex, sinkUrl, sinkUser, sinkPass, sinkDb, sinkTable);


    }

    public static void initTable(ParameterTool parameterTool) throws SQLException {

        String sourceIp = parameterTool.get("source.host");
        int sourcePort = Integer.parseInt(parameterTool.get("source.port"));
        String sourceUser = parameterTool.get("source.user");
        String sourcePass = parameterTool.get("source.pass");
        String sourceDb = parameterTool.get("source.database");
        // todo
        String sourceTable = parameterTool.get("source.table_list");
        sourceTable = sourceTable.split("\\.")[1];

        String keyIndex = "1";

        String sinkUrl = parameterTool.get("sink.jdbc-url");
        String sinkUser = parameterTool.get("sink.username");
        String sinkPass = parameterTool.get("sink.password");
        String sinkDb = parameterTool.get("sink.database-name");
        String sinkTable = parameterTool.get("sink.table-name");

        initTable(null, sourceIp, sourcePort, sourceUser, sourcePass, sourceDb, sourceTable, null, sinkUrl, sinkUser, sinkPass, sinkDb, sinkTable);

    }

    public static void initTable(String targetTableType, String sourceIp, int sourcePort, String sourceUser, String sourcePass, String sourceDb, String sourceTable, String keyIndex, String sinkUrl, String sinkUser, String sinkPass, String sinkDb, String sinkTable) throws SQLException {

        if (keyIndex == null) {
            keyIndex = "1";
        }
        if (targetTableType == null) {
            targetTableType = "PRIMARY KEY";
        }
        // check sink table not exists
//        String sinkUrl = "jdbc:mysql://" + sinkIp + ":" + sinkPort;
        Connection sinkConnect = DriverManager.getConnection(sinkUrl, sinkUser, sinkPass);
        PreparedStatement ps = sinkConnect.prepareStatement("desc " + sinkDb + "." + sinkTable);
        ResultSet resultSet = ps.executeQuery();
        if (!resultSet.next()) {
            LOG.info("sink table exists, not need create");
            return;
        }


        // load source
        TableSchema tableSchema = getSourceTableSchema(sourceIp, sourcePort, sourceUser, sourcePass, sourceDb, sourceTable);
        if (tableSchema.getColumn().isEmpty()) {
            LOG.warn("table {}.{} have not column", sourceDb, sourceTable);
        }
        String ddl = makeUpDdl(tableSchema, keyIndex, targetTableType, sinkDb, sinkTable);
        LOG.info(ddl);


        // execute ddl


        PreparedStatement ddlPs = sinkConnect.prepareStatement(ddl);
        ddlPs.execute();
        return;
    }

    private static TableSchema getSourceTableSchema(String sourceIp, int sourcePort, String sourceUser, String sourcePass, String sourceDb, String sourceTable) throws SQLException {
        String mysqlUrl = "jdbc:mysql://" + sourceIp + ":" + sourcePort;

        Connection connection = DriverManager.getConnection(mysqlUrl, sourceUser, sourcePass);

        PreparedStatement ps = connection.prepareStatement(sql);

        ps.setString(1, sourceDb);
        ps.setString(2, sourceTable);

        ResultSet resultSet = ps.executeQuery();

        TableSchema tableSchema = new TableSchema();
        while (resultSet.next()) {
            String tableName = resultSet.getString(1);
            String tableComment = resultSet.getString(2);
            String columnStr = resultSet.getString(3);
            String columnType = resultSet.getString(4);
            String columnComment = resultSet.getString(5);

            tableSchema.setTableName(tableName);
            tableSchema.setTableComment(tableComment);

            Column column = new Column(columnStr, columnType, columnComment);
            tableSchema.getColumn().add(column);
        }
        return tableSchema;
    }

    private static String makeUpDdl(TableSchema tableSchema, String keyIndex, String targetTableType, String sinkDb, String sinkTable) {

        StringBuilder builder = new StringBuilder();
        builder.append("create table " + sinkDb + "." + sinkTable + "(").append("\n");

        // add column
        for (int i = 0; i < tableSchema.getColumn().size(); i++) {

            Column column = tableSchema.getColumn().get(i);


            String columnType = parseColumnType(column.getType());


            if (i == 0) {
                builder.append(column.getName() + "\t\t" + columnType + "\t\t comment '" + column.getComment() + "'").append("\n");
            } else {
                builder.append("," + column.getName() + "\t\t" + columnType + "\t\t comment '" + column.getComment() + "'").append("\n");
            }
        }

        String key = "";
        for (int i = 0; i < Integer.parseInt(keyIndex); i++) {

            key += tableSchema.getColumn().get(i).getName() + ",";
        }
        key = key.substring(0, key.length() - 1);


        builder.append(")").append("\n");
        builder.append(targetTableType + " (" + key + ")").append("\n");
        builder.append("DISTRIBUTED BY HASH(" + key + ") BUCKETS 2").append("\n");
        builder.append("PROPERTIES(").append("\n");
        builder.append("\t'replication_num' = '1'").append("\n");
        builder.append(");").append("\n");

        return builder.toString();
    }

    private static String parseColumnType(String type) {

        if ("text".equalsIgnoreCase(type)) {
            type = "string";
        }

        return type;
    }

}
