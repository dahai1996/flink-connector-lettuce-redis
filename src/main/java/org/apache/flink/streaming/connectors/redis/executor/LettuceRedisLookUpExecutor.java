package org.apache.flink.streaming.connectors.redis.executor;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.calcite.shaded.com.google.common.cache.Cache;
import org.apache.flink.streaming.connectors.redis.config.ClientConfig;
import org.apache.flink.streaming.connectors.redis.config.LookUpConfig;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * @author sqh
 */
public interface LettuceRedisLookUpExecutor extends Function, Serializable {

    byte[] CSV_SET_PREFIX = ",".getBytes();
    byte[] CSV_HSET_PREFIX = ",,".getBytes();
    String HSET_SP = ":";

    /**
     * 打开客户端连接
     *
     * @param clientConfig 客户端属性
     * @param lookUpConfig look up属性
     */
    void open(ClientConfig clientConfig, LookUpConfig lookUpConfig);

    /** 在close的时候关闭redis连接 */
    void close();

    /**
     * 获取本地缓存
     *
     * @return cache
     */
    Cache<String, RowData> getCache();

    /**
     * 根据配置,判断是否生成本地缓存
     *
     * @return 布尔
     */
    boolean isCache();

    /**
     * 从本地缓存种判断是否有
     *
     * @param keys sql穿过来的keys
     * @return 本地缓存种的rowData
     */
    RowData getIfPresentCache(Object... keys);

    /**
     * 异步look up redis
     *
     * @param result flink结果
     * @param deserializationSchema 格式解析器
     * @param keys sql穿过来的keys
     */
    void asyncLookUpRedis(
            CompletableFuture<Collection<RowData>> result,
            DeserializationSchema<RowData> deserializationSchema,
            Object... keys);
}
