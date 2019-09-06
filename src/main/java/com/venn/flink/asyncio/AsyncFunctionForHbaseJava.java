package com.venn.flink.asyncio;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class AsyncFunctionForHbaseJava extends RichAsyncFunction<AsyncUser, AsyncUser> {

    Table table = null;
    Logger logger = LoggerFactory.getLogger(AsyncFunctionForHbaseJava.class);
    @Override
    public void open(Configuration parameters) throws Exception {
        logger.info("async function for hbase java open ...");
        super.open(parameters);
       org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        config.set(HConstants.ZOOKEEPER_QUORUM, "venn");
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);

        TableName tableName = TableName.valueOf("async");
        Connection conn = ConnectionFactory.createConnection(config);
        table = conn.getTable(tableName);
    }


    /**
     * use asyncUser.getId get asyncUser phone
     * @param asyncUser
     * @param resultFuture
     * @throws Exception
     */
    @Override
    public void asyncInvoke(AsyncUser asyncUser, ResultFuture<AsyncUser> resultFuture) throws Exception {

        Get get = new Get(asyncUser.getId().getBytes());
        get.addColumn("cf".getBytes(), "phone".getBytes());

        Result result = table.get(get);

        String phone = Bytes.toString(result.getValue("cf".getBytes(), "phone".getBytes()));

        if ( phone ==null || phone.length() != 11){
            phone = "00000000000";
        }
        asyncUser.setPhone(phone);
        List<AsyncUser> list = new ArrayList();
        list.add(asyncUser);
        resultFuture.complete(list);
    }

    @Override
    public void timeout(AsyncUser input, ResultFuture<AsyncUser> resultFuture) throws Exception {
        logger.info("Async function for hbase timeout");
        List<AsyncUser> list = new ArrayList();
        input.setPhone("00000000001");
        list.add(input);
        resultFuture.complete(list);

    }

    /**
     * close function
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
       logger.info("async function for hbase java close ...");
        super.close();
    }
}
