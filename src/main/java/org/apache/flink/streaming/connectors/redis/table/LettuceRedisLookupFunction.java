package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.redis.config.ClientConfig;
import org.apache.flink.streaming.connectors.redis.config.LookUpConfig;
import org.apache.flink.streaming.connectors.redis.executor.LettuceRedisLookUpExecutor;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * @author sqh
 */
public class LettuceRedisLookupFunction extends AsyncTableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(LettuceRedisLookupFunction.class);
    private final ClientConfig clientConfig;
    private final LookUpConfig lookUpConfig;
    private final DeserializationSchema<RowData> deserialization;
    private final LettuceRedisLookUpExecutor lookUpExecutor;

    public LettuceRedisLookupFunction(
            ClientConfig clientConfig,
            LookUpConfig lookUpConfig,
            LettuceRedisLookUpExecutor executor,
            DeserializationSchema<RowData> deserialization) {

        this.clientConfig = clientConfig;
        this.lookUpConfig = lookUpConfig;
        this.deserialization = deserialization;
        this.lookUpExecutor = executor;
    }

    public void eval(CompletableFuture<Collection<RowData>> result, Object... keys) {
        // schema 的顺序表示 redis中的键值
        // 1. set 第一位表示key，第二位表示value
        // 2. hset 第一位表示map结构的key,第二位表示field, 第三位表示value
        if (lookUpExecutor.isCache()) {
            RowData cacheRow = lookUpExecutor.getIfPresentCache(keys);
            if (cacheRow != null) {
                result.complete(Collections.singleton(cacheRow));
                return;
            }
        }
        lookUpExecutor.asyncLookUpRedis(result, deserialization, keys);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        try {
            lookUpExecutor.open(clientConfig, lookUpConfig);
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        lookUpExecutor.close();
    }
}
