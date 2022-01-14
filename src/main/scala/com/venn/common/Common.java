package com.venn.common;

import scala.Boolean;

import java.util.Properties;

/**
 * Created by venn on 19-3-5.
 */
public class Common {

    public final static String BROKER_LIST = "venn:9092";
    public final static String ZOOKEEPER_QUORUM = "venn";
    public final static String ZOOKEEPER_PORT = "2180";
    public final static String ZOOKEEPER_ZNODE_PARENT = "venn:9092";
    public final static String PULSAR_SERVER = "pulsar://localhost:6650";
    public final static String PULSAR_ADMIN = "http://localhost:8080";
    public final static String CHECK_POINT_DATA_DIR = "/home/wuxu/tmp/checkpoint";
//    public final static String CHECK_POINT_DATA_DIR = "file:///out/checkpoint";
//    public final static String CHECK_POINT_DATA_DIR = "hdfs:///venn/checkpoint";

    public static Properties prop = null;

    public static Properties getProp(String brokerList) {

        if (prop == null) {
            prop = new Properties();
            prop.put("bootstrap.servers", brokerList);
            prop.put("request.required.acks", "-1");
            prop.put("auto.offset.reset", "latest");
            prop.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            prop.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            prop.put("group.id", "venn");
            prop.put("client.id", "venn");
        }
        return prop;
    }

    public static Properties getProp() {

        return getProp(BROKER_LIST);
    }


}
