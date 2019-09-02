package com.venn.flink.asyncio;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AsyncFunctionForMysqlJava extends RichAsyncFunction<AsyncUser, AsyncUser> {


    Logger logger = LoggerFactory.getLogger(AsyncFunctionForMysqlJava.class);
    private transient MysqlClient client;
    private transient ExecutorService executorService;

    /**
     * open 方法中初始化链接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        logger.info("async function for mysql java open ...");
        super.open(parameters);

        client = new MysqlClient();
        executorService = Executors.newFixedThreadPool(30);
    }

    /**
     * use asyncUser.getId async get asyncUser phone
     *
     * @param asyncUser
     * @param resultFuture
     * @throws Exception
     */
    @Override
    public void asyncInvoke(AsyncUser asyncUser, ResultFuture<AsyncUser> resultFuture) throws Exception {

        executorService.submit(() -> {
            // submit query
            System.out.println("submit query : " + asyncUser.getId() + "-1-" + System.currentTimeMillis());
            AsyncUser tmp = client.query1(asyncUser);
            // 一定要记得放回 resultFuture，不然数据全部是timeout 的
            resultFuture.complete(Collections.singletonList(tmp));
        });
    }

    @Override
    public void timeout(AsyncUser input, ResultFuture<AsyncUser> resultFuture) throws Exception {
        logger.warn("Async function for hbase timeout");
        List<AsyncUser> list = new ArrayList();
        input.setPhone("timeout");
        list.add(input);
        resultFuture.complete(list);
    }

    /**
     * close function
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        logger.info("async function for mysql java close ...");
        super.close();
    }
}
