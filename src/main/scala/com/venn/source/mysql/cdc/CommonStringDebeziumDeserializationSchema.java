package com.venn.source.mysql.cdc;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * deserialize debezium format binlog
 */
public class CommonStringDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

    private static final Logger log = LoggerFactory.getLogger(CommonStringDebeziumDeserializationSchema.class);
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
        jsonObject.addProperty("host", host);
        // add meta
        jsonObject.addProperty("binlog", binlog);
        jsonObject.addProperty("offset", offset);
        jsonObject.addProperty("ts_sec", ts_sec);
        jsonObject.addProperty("port", port);
        jsonObject.addProperty("file", (String) record.sourceOffset().get("file"));
        jsonObject.addProperty("pos", (Long) record.sourceOffset().get("pos"));
        jsonObject.addProperty("ts_sec", (Long) record.sourceOffset().get("ts_sec"));

        if ("mysql_binlog_source".equals(record.topic())) {
            // ddl
            // todo get schame change
        } else {
            // dml
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
    }

    private JsonObject parseRecord(Struct after) {
        JsonObject jo = new JsonObject();
        for (Field field : after.schema().fields()) {

            String fieldName = field.name();
            try {
                switch ((field.schema()).type()) {
                    case INT8:
                        short resultInt8 = after.getInt8(fieldName);
                        jo.addProperty(fieldName, resultInt8);
                        break;
                    case INT16:
                        Short resultInt16 = after.getInt16(fieldName);
                        jo.addProperty(fieldName, resultInt16);
                        break;
                    case INT32:
                        Integer resultInt32 = after.getInt32(fieldName);
                        jo.addProperty(fieldName, resultInt32);
                        break;
                    case INT64:
                        Long resultInt = after.getInt64(fieldName);
                        jo.addProperty(fieldName, resultInt);
                        break;
                    case FLOAT32:
                        Float resultFloat32 = after.getFloat32(fieldName);
                        jo.addProperty(fieldName, resultFloat32);
                        break;
                    case FLOAT64:
                        Double resultFloat64 = after.getFloat64(fieldName);
                        jo.addProperty(fieldName, resultFloat64);
                        break;
                    case STRING:
                        String resultStr = after.getString(fieldName);
                        jo.addProperty(fieldName, resultStr);
                        break;
                    case BOOLEAN:
                        boolean bool = after.getBoolean(fieldName);
                        jo.addProperty(fieldName, bool);
                    case BYTES:
                        String value = null;
                        Object obj = after.get(fieldName);
                        // todo other Bytes
                        if (obj instanceof BigDecimal) {
                            jo.addProperty(fieldName, (BigDecimal) obj);
                        }
//                    jo.addProperty(fieldName, obj);
                        break;
                    default:
                }
            } catch (Exception e) {
                log.info("字段解析错误", e);
//                e.printStackTrace();
            }
        }

        return jo;
    }

    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
