package com.venn.source.cust;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadHttpWordCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.addSource(new CustHttpSource("http://localhost:8888", 10));

        source.map(item -> item)
                .keyBy(item -> "0")
                .max(0)
                .print();
        env.execute("ReadHttpWordCount");


    }
}
