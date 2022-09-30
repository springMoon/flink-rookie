package com.venn.question.cdcStarrocks;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CdcStarMapFunction extends RichMapFunction<String, CdcRecord> {

    private JsonParser parser;
    private HashMap<String, List<String>> columnMap = new HashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        parser = new JsonParser();
    }

    @Override
    public CdcRecord map(String element) throws Exception {
        JsonObject object = parser.parse(element).getAsJsonObject();
        String db = object.get("db").getAsString();
        String table = object.get("table").getAsString();
        String op = object.get("operator_type").getAsString();

        CdcRecord record = new CdcRecord(db, table, op);

        // insert/update
        String dataLocation = "after";
        if("d".equals(op)){
            // parse before
            dataLocation = "before";
        }

        JsonObject data = object.get(dataLocation).getAsJsonObject();

        for(Map.Entry<String, JsonElement> entry: data.entrySet()){

            String columnName = entry.getKey();
            String columnValue;
            JsonElement value = entry.getValue();
            if(!value.isJsonNull()){
                // if column value is not null, get as string
                columnValue = value.getAsString();
                // put column name/value to record.data
                record.getData().put(columnName, columnValue);
            }

        }

        return record;
    }
}
