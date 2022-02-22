package com.venn.demo;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class AsyncRedis extends RichAsyncFunction<String, String> {
    private RedisAsyncCommands<String, String> async;
    private List<String> nodes;
    private StatefulRedisConnection<String, String> connection;
    private RedisClient redisClient;

    public AsyncRedis(List<String> nodes) {
        this.nodes = nodes;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // redis standalone
        redisClient = RedisClient.create("redis://localhost");
        connection = redisClient.connect();

        // redis cluster
//        List<RedisURI> uriList = new ArrayList<>();
//        nodes.forEach(node -> {
//            String[] addrStr = node.split(":");
//            String host = addrStr[0];
//            int port = Integer.parseInt(addrStr[1]);
//            RedisURI redisUri = RedisURI.Builder.redis(host).withPort(port).build();
//            uriList.add(redisUri);
//        });

//        RedisClusterClient redisClient = redisClusterClient.create(uriList);
//        connection = redisClient.connect();


        // sync
//        sync = connection.sync();
        // async
        async = connection.async();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }

    //数据处理的方法
    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {

        // Redis 查询 String 类型
//        RedisFuture<String> redisFuture = async.get(input);
        // todo query hash
        RedisFuture<String> redisFuture = async.hget("key", input);
        // if input is key, get all
//        async.hgetall(input);


        //关联判断
        CompletableFuture.supplyAsync(() -> {
            try {
                return redisFuture.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            return "nothing";
        }).thenAccept(new Consumer<String>() {
            @Override
            public void accept(String result) {
                // return result
                resultFuture.complete(Collections.singleton(input + " - " + result));
            }
        });
    }
}
