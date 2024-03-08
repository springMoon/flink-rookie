package com.venn.connector.starrocks;

import java.util.ArrayList;
import java.util.List;

/**
 * @Classname TableSchema
 * @Description TODO
 * @Date 2024/3/8
 * @Created by venn
 */
public class TableSchema {

    private String tableName;
    private String tableComment;
    private List<Column> column = new ArrayList<>();

    public TableSchema() {
    }

    public TableSchema(String tableName, String tableComment, List<Column> column) {
        this.tableName = tableName;
        this.tableComment = tableComment;
        this.column = column;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableComment() {
        return tableComment;
    }

    public void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    public List<Column> getColumn() {
        return column;
    }

    public void setColumn(List<Column> column) {
        this.column = column;
    }

    @Override
    public String toString() {
        return "TableSchema{" +
                "tableName='" + tableName + '\'' +
                ", tableComment='" + tableComment + '\'' +
                ", column=" + column +
                '}';
    }
}
