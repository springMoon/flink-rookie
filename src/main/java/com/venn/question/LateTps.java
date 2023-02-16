package com.venn.question;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class LateTps {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);



    }
}
