package org.apache.flink.streaming.connectors.redis.executor;

import java.io.Serializable;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.connectors.redis.common.config.ClientConfig;
import org.apache.flink.streaming.connectors.redis.common.config.SinkConfig;
import org.apache.flink.streaming.connectors.redis.table.LettuceRedisDataType;
import org.apache.flink.streaming.connectors.redis.table.LettuceRedisDynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

/**
 * @author sqh
 */
public interface LettuceRedisSinkExecutor extends Function, Serializable {

    /**
     * 获取执行器对应的redis数据类型
     *
     * @return 枚举类 {@link LettuceRedisDataType}
     */
    LettuceRedisDataType getDataType();

    /**
     * 获取设置的redis key 超时时间
     *
     * @return 超时时间
     */
    int getTtl();

    /**
     * 是否需要field
     *
     * @return 布尔值
     */
    boolean needField();

    /**
     * 执行insert操作
     *
     * @param keyName keyName
     * @param fieldName fieldName
     * @param value value
     * @param rowKind rowKind
     * @throws InterruptedException 报错
     */
    void invoke(byte[] keyName, byte[] fieldName, byte[] value, RowKind rowKind)
            throws InterruptedException;

    /**
     * 打开redis客户端连接
     *
     * @param clientConfig 客户端属性
     * @param sinkConfig 输出属性
     */
    void open(ClientConfig clientConfig, SinkConfig sinkConfig);

    /** 在close的时候关闭redis连接 */
    void close();

    /**
     * 排除key field的数据，只保留值。 在我们构建format的时候， {@link
     * LettuceRedisDynamicTableSink#createValueFormatProjection(DataType, int)} (DataType)}我们从中剔除了
     * key,field的字段，所以此处解析的时候， 我们的rowData也需要剔除 key,field的值，后面再传给format.达到 format需要的字段与收到的值匹配的效果
     *
     * @param rowData 旧rowData
     * @return 新旧rowData，不包含key field
     */
    RowData getValueRowDate(RowData rowData);
}
