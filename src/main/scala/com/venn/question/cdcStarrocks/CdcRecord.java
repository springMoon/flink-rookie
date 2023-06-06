package com.venn.question.cdcStarrocks;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * cdcRecord save
 */
public class CdcRecord {

    private String db;
    private String table;
    private String op;
    private Map<String, String> data = new LinkedHashMap<>();

    public CdcRecord(String db, String table, String op) {
        this.db = db;
        this.table = table;
        this.op = op;
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

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public Map<String, String> getData() {
        return data;
    }

    public void setData(Map<String, String> data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "CdcRecord{" +
                "db='" + db + '\'' +
                ", table='" + table + '\'' +
                ", op='" + op + '\'' +
                ", data=" + data +
                '}';
    }
}
