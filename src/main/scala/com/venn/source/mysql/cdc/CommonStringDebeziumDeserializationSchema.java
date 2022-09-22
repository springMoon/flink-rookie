package com.venn.source.mysql.cdc;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * deserialize debezium format binlog
 */
public class CommonStringDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

    private String host;
    private int port;


    public CommonStringDebeziumDeserializationSchema(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void deserialize(SourceRecord record, Collector<String> out) {
        JsonObject jsonObject = new JsonObject();

        String binlog = record.sourceOffset().get("file").toString();
        String offset = record.sourceOffset().get("pos").toString();
        String ts_sec = record.sourceOffset().get("ts_sec").toString();

//        System.out.println("binlog : " + binlog + ", offset = " + offset);

        jsonObject.addProperty("host", host);
        // add meta
        jsonObject.addProperty("binlog", binlog);
        jsonObject.addProperty("offset", offset);
        jsonObject.addProperty("ts_sec", ts_sec);
        jsonObject.addProperty("port", port);
        jsonObject.addProperty("file", (String) record.sourceOffset().get("file"));
        jsonObject.addProperty("pos", (Long) record.sourceOffset().get("pos"));
        jsonObject.addProperty("ts_sec", (Long) record.sourceOffset().get("ts_sec"));
        String[] name = record.valueSchema().name().split("\\.");
        jsonObject.addProperty("db", name[1]);
        jsonObject.addProperty("table", name[2]);
        Struct value = ((Struct) record.value());
        String operatorType = value.getString("op");
        jsonObject.addProperty("operator_type", operatorType);
        // c : create, u: update, d: delete, r: read
        // insert update
        if (!"d".equals(operatorType)) {
            Struct after = value.getStruct("after");
            JsonObject afterJsonObject = parseRecord(after);
            jsonObject.add("after", afterJsonObject);
        }
        // update & delete
        if ("u".equals(operatorType) || "d".equals(operatorType)) {
            Struct source = value.getStruct("before");
            JsonObject beforeJsonObject = parseRecord(source);
            jsonObject.add("before", beforeJsonObject);
        }
        jsonObject.addProperty("parse_time", System.currentTimeMillis() / 1000);

        out.collect(jsonObject.toString());
    }

    private JsonObject parseRecord(Struct after) {
        JsonObject jo = new JsonObject();
        for (Field field : after.schema().fields()) {
            switch ((field.schema()).type()) {
                case INT8:
                    int resultInt8 = after.getInt8(field.name());
                    jo.addProperty(field.name(), resultInt8);
                    break;
                case INT64:
                    Long resultInt = after.getInt64(field.name());
                    jo.addProperty(field.name(), resultInt);
                    break;
                case FLOAT32:
                    Float resultFloat32 = after.getFloat32(field.name());
                    jo.addProperty(field.name(), resultFloat32);
                    break;
                case FLOAT64:
                    Double resultFloat64 = after.getFloat64(field.name());
                    jo.addProperty(field.name(), resultFloat64);
                    break;
                case BYTES:
                    // json ignore byte column
                    // byte[] resultByte = after.getBytes(field.name());
                    // jo.addProperty(field.name(), String.valueOf(resultByte));
                    break;
                case STRING:
                    String resultStr = after.getString(field.name());
                    jo.addProperty(field.name(), resultStr);
                    break;
                default:
            }
        }

        return jo;
    }

    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
