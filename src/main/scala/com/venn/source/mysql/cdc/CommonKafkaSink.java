package com.venn.source.mysql.cdc;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class CommonKafkaSink extends RichSinkFunction<String> implements CheckpointListener, CheckpointedFunction {

    protected static final Logger LOG = LoggerFactory.getLogger(CommonKafkaSink.class);
    private transient KafkaProducer<String, String> kafkaProducer;
    private transient JsonParser parser;
    //    private static final int BATCH_SIE = 100;
    private ListState<ProducerRecord<String, String>> unionOffsetStates;
    private List<ProducerRecord<String, String>> cacheList;

    @Override
    public void open(Configuration parameters) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("request.timeout.ms", "10");
        kafkaProducer = new KafkaProducer<>(prop);
        parser = new JsonParser();

    }

    @Override
    public void invoke(String element, Context context) {

        JsonObject jsonObject = parser.parse(element).getAsJsonObject();
        String db = jsonObject.get("db").getAsString();
        String table = jsonObject.get("table").getAsString();
        // topic 不存在就自动创建
        String topic = db + "_" + table;

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, element);

        cacheList.add(record);
        kafkaProducer.send(record);
        System.out.println("send message : " + element);

    }

    @Override
    public void close() {
        kafkaProducer.close();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

        this.unionOffsetStates.clear();
        if (!cacheList.isEmpty()) {
            this.unionOffsetStates.addAll(cacheList);
        }
    }


    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        OperatorStateStore stateStore = context.getOperatorStateStore();
        this.unionOffsetStates = stateStore.getUnionListState(new ListStateDescriptor("list-state", TypeInformation.of(new TypeHint<ProducerRecord>() {
        })));

        if (context.isRestored()) {
            this.cacheList = new ArrayList<>();

            for (ProducerRecord<String, String> producerRecord : this.unionOffsetStates.get()) {
                this.cacheList.add(producerRecord);
                kafkaProducer.send(producerRecord);
            }

            LOG.info("Consumer subtask {} restored state: {}.", this.getRuntimeContext().getIndexOfThisSubtask(), this.cacheList);
        } else {
            LOG.info("Consumer subtask {} has no restore state.", this.getRuntimeContext().getIndexOfThisSubtask());
        }
        kafkaProducer.beginTransaction();

    }

    @Override
    public void notifyCheckpointComplete(long l) {
        kafkaProducer.commitTransaction();
        cacheList.clear();
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        kafkaProducer.abortTransaction();
    }
}
