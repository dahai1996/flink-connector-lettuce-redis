package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.config.ClientConfig;
import org.apache.flink.streaming.connectors.redis.config.SinkConfig;
import org.apache.flink.streaming.connectors.redis.executor.LettuceRedisSinkExecutor;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author sqh
 */
public class LettuceRedisSinkFunction<IN> extends RichSinkFunction<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(LettuceRedisSinkFunction.class);
    private final ClientConfig clientConfig;
    private final SinkConfig sinkConfig;
    private final LettuceRedisSinkExecutor sinkExecutor;
    private final SerializationSchema<RowData> format;

    public LettuceRedisSinkFunction(
            ClientConfig clientConfig,
            SinkConfig sinkConfig,
            LettuceRedisSinkExecutor sinkExecutor,
            SerializationSchema<RowData> format) {
        this.clientConfig = clientConfig;
        this.sinkConfig = sinkConfig;
        this.format = format;
        this.sinkExecutor = sinkExecutor;
    }

    @Override
    public void invoke(IN input, Context context) throws Exception {
        RowData rowData = (RowData) input;
        RowKind kind = rowData.getRowKind();
        if (kind != RowKind.INSERT && kind != RowKind.UPDATE_AFTER && kind != RowKind.DELETE) {
            // 只处理 插入更改删除事件
            return;
        }
        executorInvoke(rowData);
    }

    private void executorInvoke(RowData rowData) throws InterruptedException {
        // 字段依次为redis命令的参数
        // 如hset：第一位为 key 第二位为field
        byte[] keyName = getKeyName(rowData);
        byte[] fieldName = null;
        if (sinkExecutor.needField()) {
            fieldName = getFieldName(rowData);
        }

        RowData valueRowDate = sinkExecutor.getValueRowDate(rowData);
        byte[] value = format.serialize(valueRowDate);

        sinkExecutor.invoke(keyName, fieldName, value, rowData.getRowKind());
    }

    private byte[] getKeyName(RowData rowData) {
        // 从rowData中获取。头两位为 redis key field
        return rowData.getString(0).toBytes();
    }

    private byte[] getFieldName(RowData rowData) {
        // 从rowData中获取。头两位为 redis key field
        return rowData.getString(1).toBytes();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            sinkExecutor.open(clientConfig, sinkConfig);
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        sinkExecutor.close();
    }
}
