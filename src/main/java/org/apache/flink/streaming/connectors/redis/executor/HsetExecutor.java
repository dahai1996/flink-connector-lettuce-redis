package org.apache.flink.streaming.connectors.redis.executor;

import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.calcite.shaded.com.google.common.cache.Cache;
import org.apache.flink.calcite.shaded.com.google.common.cache.CacheBuilder;
import org.apache.flink.streaming.connectors.redis.client.LettuceRedisClient;
import org.apache.flink.streaming.connectors.redis.config.ClientConfig;
import org.apache.flink.streaming.connectors.redis.config.LookUpConfig;
import org.apache.flink.streaming.connectors.redis.config.SinkConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author sqh
 */
public class HsetExecutor implements LettuceRedisSinkExecutor, LettuceRedisLookUpExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(HsetExecutor.class);
    private int ttl;
    private Cache<String, RowData> cache;
    private boolean isCache;
    private String format;
    private int maxRetryTimes;
    private LettuceRedisClient redisClient;

    @Override
    public int getTtl() {
        return this.ttl;
    }

    @Override
    public boolean needField() {
        return true;
    }

    @Override
    public void invoke(byte[] keyName, byte[] fieldName, byte[] value, RowKind rowKind)
            throws InterruptedException {
        RedisClusterCommands<byte[], byte[]> sync = redisClient.getSync();
        for (int i = 0; i <= maxRetryTimes; i++) {
            try {
                if (rowKind == RowKind.INSERT || rowKind == RowKind.UPDATE_AFTER) {
                    sync.hset(keyName, fieldName, value);
                } else if (rowKind == RowKind.DELETE) {
                    sync.hdel(keyName, fieldName);
                }
                break;
            } catch (Exception e) {
                LOG.error("sink redis error, retry times:{}", i, e);
                if (i >= maxRetryTimes) {
                    throw new RuntimeException("sink redis error ", e);
                }
                Thread.sleep(500L * i);
            }
        }
    }

    @Override
    public void open(ClientConfig clientConfig, SinkConfig sinkConfig) {
        redisClient = LettuceRedisClient.create(clientConfig);
        redisClient.open();
        this.ttl = sinkConfig.getValueTtl();
        this.maxRetryTimes = sinkConfig.getMaxRetryTimes();
    }

    @Override
    public void close() {
        redisClient.close();
    }

    @Override
    public RowData getValueRowDate(RowData rowData) {
        GenericRowData rowData1 = (GenericRowData) rowData;
        ArrayList<Object> list = new ArrayList<>();
        for (int i = 2; i < rowData1.getArity(); i++) {
            StringData string = rowData1.getString(i);
            list.add(string);
        }
        return GenericRowData.of(list.toArray());
    }

    // ------------------------------------------------------//
    //  for look up
    // ------------------------------------------------------//

    @Override
    public void open(ClientConfig clientConfig, LookUpConfig lookUpConfig) {
        redisClient = LettuceRedisClient.create(clientConfig);
        redisClient.open();
        this.cache =
                lookUpConfig.getCacheMaxRows() == -1 || lookUpConfig.getCacheTtl() == -1
                        ? null
                        : CacheBuilder.newBuilder()
                                .expireAfterWrite(lookUpConfig.getCacheTtl(), TimeUnit.SECONDS)
                                .maximumSize(lookUpConfig.getCacheMaxRows())
                                .build();
        this.isCache = this.cache != null;
        this.format = clientConfig.getFormat();
    }

    @Override
    public Cache<String, RowData> getCache() {
        return cache;
    }

    @Override
    public boolean isCache() {
        return isCache;
    }

    @Override
    public RowData getIfPresentCache(Object... keys) {
        return cache.getIfPresent(keys[0] + ":" + keys[1]);
    }

    @Override
    public void asyncLookUpRedis(
            CompletableFuture<Collection<RowData>> result,
            DeserializationSchema<RowData> deserializationSchema,
            Object... keys) {
        redisClient
                .getAsync()
                .hget(keys[0].toString().getBytes(), keys[1].toString().getBytes())
                .thenAccept(
                        value -> {
                            if (value == null) {
                                result.complete(Collections.emptyList());
                                return;
                            }
                            try {
                                RowData rowData = getRowData(value, keys, 2, deserializationSchema);
                                if (isCache) {
                                    cache.put(keys[0] + HSET_SP + keys[1], rowData);
                                }
                                result.complete(Collections.singletonList(rowData));
                            } catch (IOException e) {
                                e.printStackTrace();
                                result.complete(Collections.emptyList());
                            }
                        });
    }

    public RowData getRowData(
            byte[] value,
            Object[] keys,
            int keysLen,
            DeserializationSchema<RowData> deserializationSchema)
            throws IOException {
        // value????????????redis key ?????????,?????????????????????????????????????????????
        // csv ??????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
        if ("csv".equals(this.format)) {
            // ???????????????????????? ",,"??????????????????
            value = ArrayUtils.addAll(CSV_HSET_PREFIX, value);
        }
        GenericRowData tmp = (GenericRowData) deserializationSchema.deserialize(value);

        // ?????????????????????????????????????????????????????????????????????keyName???filedName?????????????????????
        for (int i = 0; i < keysLen; i++) {
            tmp.setField(i, StringData.fromString(keys[i].toString()));
        }
        return tmp;
    }
}
