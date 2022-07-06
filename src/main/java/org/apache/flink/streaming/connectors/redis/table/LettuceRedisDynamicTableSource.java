package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.redis.config.ClientConfig;
import org.apache.flink.streaming.connectors.redis.config.LookUpConfig;
import org.apache.flink.streaming.connectors.redis.executor.HsetExecutor;
import org.apache.flink.streaming.connectors.redis.executor.LettuceRedisLookUpExecutor;
import org.apache.flink.streaming.connectors.redis.executor.SetExecutor;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;

import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/**
 * @author sqh
 */
public class LettuceRedisDynamicTableSource implements ScanTableSource, LookupTableSource {

    private final ResolvedSchema resolvedSchema;
    private final ClientConfig clientConfig;
    private final LookUpConfig lookUpConfig;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final LettuceRedisLookUpExecutor executor;

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        return AsyncTableFunctionProvider.of(
                new LettuceRedisLookupFunction(
                        clientConfig,
                        lookUpConfig,
                        executor,
                        createDeserialization(
                                lookupContext,
                                decodingFormat,
                                createValueFormatProjection(
                                        resolvedSchema.toPhysicalRowDataType()))));
    }

    public LettuceRedisDynamicTableSource(
            ResolvedSchema resolvedSchema,
            ClientConfig clientConfig,
            LookUpConfig lookUpConfig,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {

        this.clientConfig = clientConfig;
        this.lookUpConfig = lookUpConfig;
        this.decodingFormat = decodingFormat;
        this.resolvedSchema = resolvedSchema;
        this.executor = getExecutorByCommand(clientConfig.getCommand());
    }

    @Override
    public DynamicTableSource copy() {
        return new LettuceRedisDynamicTableSource(
                resolvedSchema, clientConfig, lookUpConfig, decodingFormat);
    }

    @Override
    public String asSummaryString() {
        return "LettuceRedis";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        return null;
    }

    private DeserializationSchema<RowData> createDeserialization(
            Context context,
            DecodingFormat<DeserializationSchema<RowData>> format,
            int[] projection) {
        if (format == null) {
            return null;
        }
        DataType physicalFormatDataType =
                DataTypeUtils.projectRow(this.resolvedSchema.toPhysicalRowDataType(), projection);

        return format.createRuntimeDecoder(context, physicalFormatDataType);
    }

    public static int[] createValueFormatProjection(DataType physicalDataType) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                hasRoot(physicalType, LogicalTypeRoot.ROW), "Row data type expected.");
        final int physicalFieldCount = LogicalTypeChecks.getFieldCount(physicalType);
        final IntStream physicalFields = IntStream.range(0, physicalFieldCount);

        return physicalFields.toArray();
    }

    private LettuceRedisLookUpExecutor getExecutorByCommand(String command) {
        switch (command) {
            case "set":
                return new SetExecutor();
            case "hset":
                return new HsetExecutor();
            default:
                return null;
        }
    }
}
