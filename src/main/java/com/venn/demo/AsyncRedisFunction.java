package com.venn.demo;

import com.google.gson.JsonParser;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * async redis function
 */
public class AsyncRedisFunction extends RichAsyncFunction<String, String> {
    private RedisAsyncCommands<String, String> async;
    private String url;
    private StatefulRedisConnection<String, String> connection;
    private RedisClient redisClient;
    private JsonParser jsonParser;

    public AsyncRedisFunction(String url) {
        this.url = url;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // redis standalone
        redisClient = RedisClient.create(url);
        connection = redisClient.connect();

        // redis cluster
//        List<RedisURI> uriList = new ArrayList<>();
//        for (String tmp : url.split(",")) {
//            String[] str = tmp.split(":");
//            String host = str[0];
//            int port = Integer.parseInt(str[1]);
//            RedisURI redisUri = RedisURI.Builder.redis(host).withPort(port).build();
//            uriList.add(redisUri);
//        }
//        RedisClusterClient redisClient = redisClusterClient.create(uriList);
//        connection = redisClient.connect();

        // async
        async = connection.async();

        jsonParser = new JsonParser();
    }


    //数据处理的方法
    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {

        String userId = jsonParser.parse(input).getAsJsonObject().get("user_id").getAsString();
        // query string
        RedisFuture<String> redisFuture = async.get(userId);
        //  query hash
//        RedisFuture<String> redisFuture = async.hget("key", input);
        // get all
//        async.hgetall(input);

        // async query and get result
        CompletableFuture.supplyAsync(() -> {
            try {
                return redisFuture.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            // if get exception
            return "exception";
        }).thenAccept(new Consumer<String>() {
            @Override
            public void accept(String result) {
                if (result == null) {
                    result = "nothing";
                }
                // return result
                resultFuture.complete(Collections.singleton(input + " - " + result));
            }
        });
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
}
