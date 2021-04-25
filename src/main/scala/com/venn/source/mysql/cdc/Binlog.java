package com.venn.source.mysql.cdc;

public class Binlog {
    private String host;
    private int port;
    private String db;
    private String table;
    private String file;
    private Long pos;
    private Long tsSec;
    private String operatorType;
    private String data;
    private String source;

    public Binlog() {
    }

    public Binlog(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public Long getTsSec() {
        return tsSec;
    }

    public void setTsSec(Long tsSec) {
        this.tsSec = tsSec;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public Long getPos() {
        return pos;
    }

    public void setPos(Long pos) {
        this.pos = pos;
    }

    public String getOperatorType() {
        return operatorType;
    }

    public void setOperatorType(String operatorType) {
        this.operatorType = operatorType;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
