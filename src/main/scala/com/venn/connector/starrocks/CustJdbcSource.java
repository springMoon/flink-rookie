package com.venn.connector.starrocks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class CustJdbcSource extends RichSourceFunction<String> {

    private String ip;
    private String port;
    private String user;
    private String pass;
    private String sql;
    private String colSep;
    private int batch;
    private int interval;
    private boolean flag = false;
    private transient Counter counter;
    private Random random = new Random();

    private List<String> cache = new ArrayList<>();

    public CustJdbcSource(String ip, String port, String user, String pass, String sql, String colSep, int batch, int interval) {
        this.ip = ip;
        this.port = port;
        this.user = user;
        this.pass = pass;
        this.sql = sql;
        this.colSep = colSep;
        this.batch = batch;
        this.interval = interval * 1000;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        flag = true;

        counter = new SimpleCounter();
        this.counter = getRuntimeContext()
                .getMetricGroup()
                .counter("myCounter");
        // load data

        String url = "jdbc:mysql://" + ip + ":" + port;


        Connection connection = DriverManager.getConnection(url, this.user, this.pass);

        PreparedStatement ps = connection.prepareStatement(sql);

        ResultSet rs = ps.executeQuery();

        int columnCount = rs.getMetaData().getColumnCount();

        while (rs.next()) {

            StringBuilder builder = new StringBuilder();
            for (int j = 1; j <= columnCount; j++) {
                if (j == columnCount) {
                    builder.append(rs.getString(j));
                } else {
                    builder.append(rs.getString(j)).append(this.colSep);
                }
            }

            cache.add(builder.toString());
        }

        System.out.println("load cache size : " + cache.size());

    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        int dataSize = cache.size();
        while (flag) {
            int select = random.nextInt(dataSize);

            String data = cache.get(select);

            counter.inc();
            ctx.collect(data);

            if (counter.getCount() % batch == 0) {
                Thread.sleep(interval);
            }
        }

    }

    @Override
    public void cancel() {
        flag = false;
    }
}
