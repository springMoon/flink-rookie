package com.venn.connector.starrocks;

/**
 * @Classname Column
 * @Description TODO
 * @Date 2024/3/8
 * @Created by venn
 */
public class Column {

    private String name;
    private String type;
    private String comment;

    public Column() {
    }

    public Column(String name, String type, String comment) {
        this.name = name;
        this.type = type;
        this.comment = comment;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public String toString() {
        return "Column{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", comment='" + comment + '\'' +
                '}';
    }
}
