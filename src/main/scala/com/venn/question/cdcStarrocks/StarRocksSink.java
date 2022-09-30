package com.venn.question.cdcStarrocks;

import org.apache.commons.codec.binary.Base64;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.http.HttpException;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StarRocksSink extends RichSinkFunction<List<CdcRecord>> {

    private final static Logger LOG = LoggerFactory.getLogger(StarRocksSink.class);
    public final static String COL_SEP = "\\\\x01";
    public final static String ROW_SEP = "\\\\x02";
    public final static String NULL_COL = "\\N";
    private String ip;
    private String port;
    private String loadPort;
    private String user;
    private String pass;
    private String db;
    private Connection connection;
    private Map<String, String> spliceColumnMap = new HashMap<>();
    private Map<String, List<String>> columnMap = new HashMap<>();

    public StarRocksSink() {
    }

    public StarRocksSink(String ip, String port, String loadPort, String user, String pass, String db) {
        this.ip = ip;
        this.port = port;
        this.loadPort = loadPort;
        this.user = user;
        this.pass = pass;
        this.db = db;

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        reConnect();
    }

    @Override
    public void invoke(List<CdcRecord> element, Context context) throws Exception {

        LOG.info("write batch size: " + element.size());

        // use StarRocks db name
//        String db = cache.get(0).getDb();
        String table = element.get(0).getTable();
        String key = db + "_" + table;

        // get table column
        List<String> columnList = null;
        if (!columnMap.containsKey(key)) {
            // db.table is first coming, load column, put to spliceColumnMap & columnMap
            loadTargetTableSchema(key, db, table);
        }
        String columns = spliceColumnMap.get(key);
        columnList = columnMap.get(key);
        if (columnList.size() == 0) {
            LOG.info("{}.{} not exists in target starrocks, ingore data change", db, table);
        }
        String data = parseUploadData(element, columnList);

        final String loadUrl = String.format("http://%s:%s/api/%s/%s/_stream_load", ip, loadPort, db, table);
        String label = db + "_" + table + "_" + System.currentTimeMillis();

        doHttp(loadUrl, data, label, columns);

    }

    /**
     * http send data to starrocks
     */
    private void doHttp(String loadUrl, String data, String label, String columns) throws IOException, SQLException {

        final HttpClientBuilder httpClientBuilder = HttpClients
                .custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                })
                .addInterceptorFirst(new ContentLengthHeaderRemover());

        try (CloseableHttpClient client = httpClientBuilder.build()) {
            HttpPut put = new HttpPut(loadUrl);
            StringEntity entity = new StringEntity(data, "UTF-8");
            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(user, pass));
            // the label header is optional, not necessary
            // use label header can ensure at most once semantics
            put.setHeader("label", label);
            put.setHeader("columns", columns);
            put.setHeader("row_delimiter", ROW_SEP);
            put.setHeader("column_separator", COL_SEP);
            put.setEntity(entity);

            try (CloseableHttpResponse response = client.execute(put)) {
                String loadResult = "";
                if (response.getEntity() != null) {
                    loadResult = EntityUtils.toString(response.getEntity());
                }
                final int statusCode = response.getStatusLine().getStatusCode();
                // statusCode 200 just indicates that starrocks be service is ok, not stream load
                // you should see the output content to find whether stream load is success
                if (statusCode != 200) {
                    throw new IOException(
                            String.format("Stream load failed, statusCode=%s load result=%s", statusCode, loadResult));
                }

            }
        }

    }

    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    private String parseUploadData(List<CdcRecord> cache, List<String> columnList) {
        StringBuilder builder = new StringBuilder();
        for (CdcRecord element : cache) {

            Map<String, String> data = element.getData();

            for (String column : columnList) {

                if (data.containsKey(column)) {
                    builder.append(data.get(column)).append(COL_SEP);
                } else {
                    builder.append(NULL_COL).append(COL_SEP);
                }
            }
            // add _op
            if ("d".equals(element.getOp())) {
                // delete
                builder.append("1");
            } else {
                // upsert
                builder.append("0");
            }
            builder.append(ROW_SEP);
        }
        // remove last row sep
        builder = builder.delete(builder.length() - 5, builder.length());
        String data = builder.toString();
        return data;
    }

    /**
     * load table schema, parse to http column and column list for load source data
     */
    private void loadTargetTableSchema(String key, String db, String table) throws SQLException {

        List<String> columnList = new ArrayList<>();

        StringBuilder builer = new StringBuilder();
        try {
            // load table schema
            PreparedStatement insertPS = connection.prepareStatement("desc " + db + "." + table);
            ResultSet result = insertPS.executeQuery();
            while (result.next()) {

                String column = result.getString(1);
                builer.append(column).append(",");
                columnList.add(column);
            }
        } catch (SQLException e) {
            LOG.warn("load {}.{} schema error. {}", db, table, e.getStackTrace());
        }
        builer.append("__op");

        String columns = builer.toString();

        spliceColumnMap.put(key, columns);
        columnMap.put(key, columnList);
    }

    /**
     * reconnect to starrocks
     *
     * @throws SQLException
     */
    private void reConnect() throws SQLException {
        String driver = "jdbc:mysql://" + ip + ":" + port;
        if (connection == null || connection.isClosed()) {
            connection = DriverManager.getConnection(driver, user, pass);
        }
    }

    @Override
    public void finish() throws Exception {
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    private static class ContentLengthHeaderRemover implements HttpRequestInterceptor {
        @Override
        public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
            // fighting org.apache.http.protocol.RequestContent's ProtocolException("Content-Length header already present");
            request.removeHeaders(HTTP.CONTENT_LEN);
        }
    }
}
