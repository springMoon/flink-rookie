package com.venn.flink.asyncio;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlData {

    private static String jdbcUrl = "jdbc:mysql://192.168.229.128:3306?useSSL=false&allowPublicKeyRetrieval=true";
    private static String username = "root";
    private static String password = "123456";
    private static String driverName = "com.mysql.jdbc.Driver";


    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        java.sql.Connection conn;
        PreparedStatement ps;

        Class.forName(driverName);
        conn = DriverManager.getConnection(jdbcUrl, username, password);
        ps = conn.prepareStatement("insert into async.async_test(id, phone) values (?, ?)");

        for (int i = 100000; i < 1000000; i++){
            ps.setString(1, "" + i);
            ps.setString(2, "" + System.currentTimeMillis());

            ps.execute();
//            conn.commit();
        }


    }
}
