package org.apache.flink.streaming.connectors.redis.client;

import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.apache.flink.streaming.connectors.redis.config.ClientConfig;

/**
 * @author sqh
 */
public interface LettuceRedisClient {

    /** 打开客户端 */
    void open();

    /** 关闭客户端连接 */
    void close();

    /**
     * 获取同步执行命令
     *
     * @return 同步命令执行器
     */
    RedisClusterCommands<byte[], byte[]> getSync();

    /**
     * 获取异步执行命令
     *
     * @return 异步命令执行器
     */
    RedisClusterAsyncCommands<byte[], byte[]> getAsync();

    /**
     * 根据配置创建客户端
     *
     * @param clientConfig 客户端配置
     * @return 单机客户端或者集群客户端
     */
    static LettuceRedisClient create(ClientConfig clientConfig) {
        String model = clientConfig.getModel();
        if ("single".equals(model)) {
            return new SingleRedisClient(clientConfig);
        } else if ("cluster".equals(model)) {
            return new ClusterRedisClient(clientConfig);
        } else {
            throw new RuntimeException("unknown redis model:" + model);
        }
    }
}
