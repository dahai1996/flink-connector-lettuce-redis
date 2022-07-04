package org.apache.flink.streaming.connectors.redis.table;

import java.util.HashSet;
import java.util.Set;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.ClientConfig;
import org.apache.flink.streaming.connectors.redis.common.config.LookUpConfig;
import org.apache.flink.streaming.connectors.redis.common.config.SinkConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

/**
 * @author sqh
 */
public class LettuceRedisDynamicTableFactory
        implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    public static final String SINK_PREFIX = "sink.";
    public static final String LOOKUP_PREFIX = "lookup.";

    public static final ConfigOption<Integer> DATABASE =
            ConfigOptions.key("database")
                    .intType()
                    .defaultValue(0)
                    .withDescription("Optional database for connect to redis");

    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("redis hostName");

    public static final ConfigOption<String> COMMAND =
            ConfigOptions.key("command")
                    .stringType()
                    .defaultValue("set")
                    .withDescription("redis command: set or hset");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(6379)
                    .withDescription("redis port,default 6379");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(1)
                    .withDescription("sink redis retry time,default 1");

    public static final ConfigOption<Integer> SINK_VALUE_TTL =
            ConfigOptions.key("sink.value.ttl")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "sink redis ttl time,only support when command is 'set',default 0 means no expire time");

    public static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("sink.parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Optional parallelism for sink redis");

    public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS =
            ConfigOptions.key("lookup.cache.max-rows")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription("Optional  max rows of cache for query redis");

    public static final ConfigOption<Long> LOOKUP_CACHE_TTL =
            ConfigOptions.key("lookup.cache.ttl")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription("Optional ttl of cache for query redis");

    public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES =
            ConfigOptions.key("lookup.max-retries")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Optional max retries of cache for query redis");

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // discover a suitable decoding format
        // 目前支持： json,csv
        ReadableConfig config = helper.getOptions();
        String formatConfig = config.get(FactoryUtil.FORMAT);
        String commandString = config.get(COMMAND);
        if (!supportFormatOptions().contains(formatConfig)) {
            throw new ValidationException(
                    String.format(
                            "do not support this format : %s ,only support format : json,csv.",
                            formatConfig));
        }
        // 格式相关检查
        if (!supportCommandOptions().contains(commandString)) {
            throw new ValidationException(
                    String.format(
                            "do not support this command : %s ,only support command : set,hset.",
                            commandString));
        }
        // hset 模式不支持设置expire
        Integer integer = config.get(SINK_VALUE_TTL);
        if (integer != 0 && LettuceRedisDataType.HSET.getName().equals(commandString)) {
            throw new ValidationException(
                    "when command is 'hset',do not support 'sink.value.ttl'.You can set 'sink.value.ttl' to 0,it means no expire time");
        }
        EncodingFormat<SerializationSchema<RowData>> format =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT);

        helper.validateExcept(LOOKUP_PREFIX);

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        // 构造配置
        ClientConfig clientConfig = getClientConfig(config);
        SinkConfig sinkConfig = getSinkConfig(config);

        return new LettuceRedisDynamicTableSink(resolvedSchema, clientConfig, sinkConfig, format);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // discover a suitable decoding format
        // 目前支持： json,csv
        ReadableConfig config = helper.getOptions();
        String formatConfig = config.get(FactoryUtil.FORMAT);
        String commandString = config.get(COMMAND);
        if (!supportFormatOptions().contains(formatConfig)) {
            throw new ValidationException(
                    String.format(
                            "do not support this format : %s ,only support format : json,csv.",
                            formatConfig));
        }
        // 格式相关检查
        if (!supportCommandOptions().contains(commandString)) {
            throw new ValidationException(
                    String.format(
                            "do not support this command : %s ,only support command : set,hset.",
                            commandString));
        }

        DecodingFormat<DeserializationSchema<RowData>> format =
                helper.discoverDecodingFormat(
                        DeserializationFormatFactory.class, FactoryUtil.FORMAT);

        // validate  options except sink.* or json.* or csv.*
        helper.validateExcept(SINK_PREFIX);

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();

        // 构造配置
        ClientConfig clientConfig = getClientConfig(config);
        LookUpConfig lookUpConfig = getLookUpConfig(config);

        return new LettuceRedisDynamicTableSource(
                resolvedSchema, clientConfig, lookUpConfig, format);
    }

    @Override
    public String factoryIdentifier() {
        return "lettuce-redis";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(COMMAND);
        options.add(FactoryUtil.FORMAT);

        return options;
    }

    public Set<String> supportFormatOptions() {
        final Set<String> options = new HashSet<>();
        options.add("json");
        options.add("csv");

        return options;
    }

    public Set<String> supportCommandOptions() {
        final Set<String> options = new HashSet<>();
        options.add(LettuceRedisDataType.SET.getName());
        options.add(LettuceRedisDataType.HSET.getName());

        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DATABASE);
        options.add(HOSTNAME);
        options.add(PORT);
        options.add(COMMAND);
        options.add(LOOKUP_CACHE_MAX_ROWS);
        options.add(LOOKUP_CACHE_TTL);
        options.add(LOOKUP_MAX_RETRIES);
        options.add(SINK_PARALLELISM);
        options.add(SINK_VALUE_TTL);
        options.add(SINK_MAX_RETRIES);

        return options;
    }

    private SinkConfig getSinkConfig(ReadableConfig config) {
        return new SinkConfig.Builder()
                .setMaxRetryTimes(config.get(SINK_MAX_RETRIES))
                .setValueTtl(config.get(SINK_VALUE_TTL))
                .setParallelism(config.get(SINK_PARALLELISM))
                .build();
    }

    private ClientConfig getClientConfig(ReadableConfig config) {
        return new ClientConfig.Builder()
                .setHostname(config.get(HOSTNAME))
                .setPort(config.get(PORT))
                .setCommand(config.get(COMMAND))
                .setFormat(config.get(FactoryUtil.FORMAT))
                .setDatabase(config.get(DATABASE))
                .build();
    }

    private LookUpConfig getLookUpConfig(ReadableConfig config) {
        return new LookUpConfig.Builder()
                .setCacheMaxRows(config.get(LOOKUP_CACHE_MAX_ROWS))
                .setCacheTtl(config.get(LOOKUP_CACHE_TTL))
                .build();
    }
}
