package org.apache.flink.streaming.connectors.redis.client;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import org.apache.flink.streaming.connectors.redis.config.ClientConfig;

/**
 * @author sqh
 */
public class SingleRedisClient implements LettuceRedisClient {

    ClientConfig clientConfig;
    RedisClient redisClient;
    StatefulRedisConnection<byte[], byte[]> connect;
    private RedisCommands<byte[], byte[]> sync;
    private RedisAsyncCommands<byte[], byte[]> async;

    public SingleRedisClient(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    @Override
    public void open() {
        int[] ports = clientConfig.getPorts();
        String[] hosts = clientConfig.getHosts();
        if (ports.length != 1 || hosts.length != 1) {
            throw new RuntimeException("for single redis client,hosts and ports must be single");
        }
        RedisURI uri =
                RedisURI.Builder.redis(hosts[0], ports[0])
                        .withDatabase(clientConfig.getDatabase())
                        .withPassword(clientConfig.getPassword().toCharArray())
                        .build();
        this.redisClient = RedisClient.create(uri);
        this.connect = redisClient.connect(new ByteArrayCodec());
    }

    @Override
    public void close() {
        if (connect != null) {
            connect.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }

    @Override
    public RedisClusterCommands<byte[], byte[]> getSync() {
        if (sync == null) {
            sync = connect.sync();
        }
        return sync;
    }

    @Override
    public RedisClusterAsyncCommands<byte[], byte[]> getAsync() {
        if (async == null) {
            async = connect.async();
        }
        return async;
    }
}
