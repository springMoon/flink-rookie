package com.venn.demo;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * source blog: https://blog.csdn.net/znmdwzy/article/details/107849188
 * <p>
 * sync
 */
public class RedisSide extends RichAsyncFunction<String, String> {
    //构建Redis异步客户端
    private RedisClusterClient redisClusterClient;
    private StatefulRedisClusterConnection<String, String> connection;
    private RedisClusterCommands<String, String> sync;
    private List<String> nodes;

    public RedisSide(List<String> nodes) {
        this.nodes = nodes;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //给异步客户端初始化
        List<RedisURI> uriList = new ArrayList<>();
        nodes.forEach(node -> {
            String[] addrStr = node.split(":");
            String host = addrStr[0];
            int port = Integer.parseInt(addrStr[1]);
            RedisURI redisUri = RedisURI.Builder.redis(host).withPort(port).build();
            uriList.add(redisUri);
        });
        RedisClient redisClient = RedisClient.create("redis://localhost");
        StatefulRedisConnection<String, String> connection = redisClient.connect();


//        RedisClusterClient redisClient = redisClusterClient.create(uriList);
//        connection = redisClient.connect();
        sync = connection.sync();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (redisClusterClient != null) {
            redisClusterClient.shutdown();
        }
    }

    //数据处理的方法
    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
        //Rdis查询
        Map<String, String> hgetall = sync.hgetall(input);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(input);

        //关联判断
        CompletableFuture.supplyAsync(new Supplier<Map>() {
            @Override
            public Map get() {
                return hgetall;
            }
        }).thenAccept(new Consumer<Map>() {
            @Override
            public void accept(Map map) {
                if (map == null || map.size() == 0) {
                    resultFuture.complete(Collections.singleton(input));
                }
                map.forEach((key, value) -> {
                    if ("key2".equals(key)) {
                        stringBuilder.append(value);
                        resultFuture.complete(Collections.singleton(stringBuilder.toString()));
                    }
                });
            }
        });
    }
}
