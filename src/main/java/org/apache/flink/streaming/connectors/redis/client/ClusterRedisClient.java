package org.apache.flink.streaming.connectors.redis.client;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import org.apache.flink.streaming.connectors.redis.config.ClientConfig;

import java.util.ArrayList;

/**
 * @author sqh
 */
public class ClusterRedisClient implements LettuceRedisClient {

    ClientConfig clientConfig;
    private RedisAdvancedClusterAsyncCommands<byte[], byte[]> clusterAsync;
    private RedisAdvancedClusterCommands<byte[], byte[]> clusterSync;
    private StatefulRedisClusterConnection<byte[], byte[]> clusterConnection;
    private RedisClusterClient redisClusterClient;

    public ClusterRedisClient(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    @Override
    public void open() {
        int[] ports = clientConfig.getPorts();
        String[] hosts = clientConfig.getHosts();
        if (hosts.length != ports.length) {
            throw new RuntimeException("hosts and ports must be equal");
        }
        ArrayList<RedisURI> urlList = new ArrayList<>();
        for (int i = 0; i < hosts.length; i++) {
            RedisURI uri =
                    RedisURI.Builder.redis(hosts[i], ports[i])
                            .withDatabase(clientConfig.getDatabase())
                            .withPassword(clientConfig.getPassword().toCharArray())
                            .build();
            urlList.add(uri);
        }
        redisClusterClient = RedisClusterClient.create(urlList);
        clusterConnection = redisClusterClient.connect(new ByteArrayCodec());
    }

    @Override
    public void close() {
        if (clusterConnection != null) {
            clusterConnection.close();
        }
        if (redisClusterClient != null) {
            redisClusterClient.shutdown();
        }
    }

    @Override
    public RedisClusterCommands<byte[], byte[]> getSync() {
        if (clusterSync == null) {
            clusterSync = clusterConnection.sync();
        }
        return clusterSync;
    }

    @Override
    public RedisClusterAsyncCommands<byte[], byte[]> getAsync() {
        if (clusterAsync == null) {
            clusterAsync = clusterConnection.async();
        }
        return clusterAsync;
    }
}
