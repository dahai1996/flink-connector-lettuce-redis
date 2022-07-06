package org.apache.flink.streaming.connectors.redis.client;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.apache.flink.streaming.connectors.redis.config.ClientConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClusterRedisClientTest {

    private static LettuceRedisClient lettuceRedisClient;
    int[] ports = {6381, 6382, 6383, 6384, 6385, 6386};
    String[] hosts = {"node", "node", "node", "node", "node", "node"};

    @AfterClass
    public static void close() {
        lettuceRedisClient.close();
    }

    @Before
    public void open() {

        ClientConfig config =
                new ClientConfig.Builder()
                        .setModel("cluster")
                        .setPassword("123456")
                        .setPorts(ports)
                        .setHosts(hosts)
                        .setCommand("set")
                        .setDatabase(0)
                        .setFormat("csv")
                        .build();
        lettuceRedisClient = LettuceRedisClient.create(config);
        lettuceRedisClient.open();
    }

    @Test
    public void getSyncCommands() {
        RedisClusterCommands<byte[], byte[]> sync = lettuceRedisClient.getSync();

        sync.set("set1".getBytes(), "setv1".getBytes());
        sync.hset("hset1".getBytes(), "field1".getBytes(), "hsetV1".getBytes());

        Assert.assertEquals("setv1", new String(sync.get("set1".getBytes())));
        Assert.assertEquals(
                "hsetV1", new String(sync.hget("hset1".getBytes(), "field1".getBytes())));
    }

    @Test
    public void getAsyncCommands() {
        RedisClusterAsyncCommands<byte[], byte[]> async = lettuceRedisClient.getAsync();
        async.set("set2".getBytes(), "setV2".getBytes());
        async.hset("hset2".getBytes(), "field1".getBytes(), "hsetV2".getBytes());

        RedisFuture<byte[]> redisFuture = async.get("set2".getBytes());

        final String[] r1 = new String[1];
        final String[] r2 = {null};
        redisFuture
                .thenAccept(bytes -> r1[0] = new String(bytes))
                .thenAccept(unused -> Assert.assertEquals("setV2", r1[0]));

        RedisFuture<byte[]> hget = async.hget("hset2".getBytes(), "field1".getBytes());
        hget.thenAccept(bytes -> r2[0] = new String(bytes))
                .thenAccept(unused -> Assert.assertEquals("hsetV2", r2[0]));
    }
}
