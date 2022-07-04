package org.apache.flink.streaming.connectors.redis.table;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

import java.util.stream.IntStream;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.redis.common.config.ClientConfig;
import org.apache.flink.streaming.connectors.redis.common.config.SinkConfig;
import org.apache.flink.streaming.connectors.redis.executor.HsetExecutor;
import org.apache.flink.streaming.connectors.redis.executor.LettuceRedisSinkExecutor;
import org.apache.flink.streaming.connectors.redis.executor.SetExecutor;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

/**
 * @author sqh
 */
public class LettuceRedisDynamicTableSink implements DynamicTableSink {

    private final int formatStartNum;
    private final LettuceRedisSinkExecutor executor;
    private final ClientConfig clientConfig;
    private final SinkConfig sinkConfig;
    private final Integer sinkParallelism;
    private final ResolvedSchema resolvedSchema;
    private final EncodingFormat<SerializationSchema<RowData>> format;

    public LettuceRedisDynamicTableSink(
            ResolvedSchema resolvedSchema,
            ClientConfig clientConfig,
            SinkConfig sinkConfig,
            EncodingFormat<SerializationSchema<RowData>> format) {
        this.clientConfig = clientConfig;
        this.sinkConfig = sinkConfig;
        this.sinkParallelism = sinkConfig.getParallelism();
        this.executor = getExecutorByCommand(clientConfig.getCommand());
        this.formatStartNum = getFormatStartNumByCommand(clientConfig.getCommand());
        this.format = format;
        this.resolvedSchema = resolvedSchema;
    }

    private LettuceRedisSinkExecutor getExecutorByCommand(String command) {
        switch (command) {
            case "set":
                return new SetExecutor();
            case "hset":
                return new HsetExecutor();
            default:
                return null;
        }
    }

    private int getFormatStartNumByCommand(String command) {
        switch (command) {
            case "set":
                return 1;
            case "hset":
                return 2;
            default:
                return 0;
        }
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DataType physicalFormatDataType =
                DataTypeUtils.projectRow(
                        this.resolvedSchema.toPhysicalRowDataType(),
                        createValueFormatProjection(
                                resolvedSchema.toPhysicalRowDataType(), formatStartNum));

        SerializationSchema<RowData> runtimeEncoder =
                format.createRuntimeEncoder(context, physicalFormatDataType);

        return SinkFunctionProvider.of(
                new LettuceRedisSinkFunction<>(clientConfig, sinkConfig, executor, runtimeEncoder),
                sinkParallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new LettuceRedisDynamicTableSink(resolvedSchema, clientConfig, sinkConfig, format);
    }

    public static int[] createValueFormatProjection(DataType physicalDataType, int startNum) {
        // 此处时传给format哪些字段需要进行format，返回 需要format字段的序号 从0开始
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                hasRoot(physicalType, LogicalTypeRoot.ROW), "Row data type expected.");
        final int physicalFieldCount = LogicalTypeChecks.getFieldCount(physicalType);

        final IntStream physicalFields = IntStream.range(startNum, physicalFieldCount);

        return physicalFields.toArray();
    }

    @Override
    public String asSummaryString() {
        return "LETTUCE-REDIS";
    }
}
