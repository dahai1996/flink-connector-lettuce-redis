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
public class SetExecutor implements LettuceRedisSinkExecutor, LettuceRedisLookUpExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(SetExecutor.class);
    private LettuceRedisClient redisClient;
    private int ttl;
    private Cache<String, RowData> cache;
    private boolean isCache;
    private String format;
    private int maxRetryTimes;

    @Override
    public int getTtl() {
        return this.ttl;
    }

    @Override
    public boolean needField() {
        return false;
    }

    @Override
    public void invoke(byte[] keyName, byte[] fieldName, byte[] value, RowKind rowKind)
            throws InterruptedException {
        RedisClusterCommands<byte[], byte[]> sync = redisClient.getSync();
        for (int i = 0; i <= maxRetryTimes; i++) {
            try {
                if (rowKind == RowKind.INSERT || rowKind == RowKind.UPDATE_AFTER) {
                    sync.set(keyName, value);
                    if (ttl != 0) {
                        sync.expire(keyName, ttl);
                    }
                } else if (rowKind == RowKind.DELETE) {
                    sync.del(keyName);
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

        if (rowKind == RowKind.INSERT || rowKind == RowKind.UPDATE_AFTER) {
            sync.set(keyName, value);
        } else if (rowKind == RowKind.DELETE) {
            sync.del(keyName);
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
        for (int i = 1; i < rowData1.getArity(); i++) {
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
        return cache.getIfPresent(String.valueOf(keys[0]));
    }

    @Override
    public void asyncLookUpRedis(
            CompletableFuture<Collection<RowData>> result,
            DeserializationSchema<RowData> deserializationSchema,
            Object... keys) {
        redisClient
                .getAsync()
                .get(keys[0].toString().getBytes())
                .thenAccept(
                        value -> {
                            if (value == null) {
                                result.complete(Collections.emptyList());
                                return;
                            }
                            try {
                                RowData rowData = getRowData(value, keys, 1, deserializationSchema);
                                if (isCache) {
                                    cache.put(String.valueOf(keys[0]), rowData);
                                }
                                result.complete(Collections.singletonList(rowData));
                            } catch (IOException e) {
                                result.complete(Collections.emptyList());
                                e.printStackTrace();
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
            value = ArrayUtils.addAll(CSV_SET_PREFIX, value);
        }
        GenericRowData tmp = (GenericRowData) deserializationSchema.deserialize(value);

        // ?????????????????????????????????????????????????????????????????????keyName???filedName?????????????????????
        for (int i = 0; i < keysLen; i++) {
            tmp.setField(i, StringData.fromString(keys[i].toString()));
        }
        return tmp;
    }
}
