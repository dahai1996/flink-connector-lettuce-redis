package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.config.ClientConfig;
import org.apache.flink.streaming.connectors.redis.config.LookUpConfig;
import org.apache.flink.streaming.connectors.redis.config.SinkConfig;
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.streaming.connectors.redis.validate.ValidateUtils.validateCommand;
import static org.apache.flink.streaming.connectors.redis.validate.ValidateUtils.validateFormat;
import static org.apache.flink.streaming.connectors.redis.validate.ValidateUtils.validateModel;
import static org.apache.flink.streaming.connectors.redis.validate.ValidateUtils.validateTtl;

/**
 * @author sqh
 */
public class LettuceRedisDynamicTableFactory
        implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    public static final String SINK_PREFIX = "sink.";
    public static final String LOOKUP_PREFIX = "lookup.";

    public static final ConfigOption<String> MODEL =
            ConfigOptions.key("client.model")
                    .stringType()
                    .defaultValue("single")
                    .withDescription("redis model: single or cluster");
    public static final ConfigOption<Integer> DATABASE =
            ConfigOptions.key("client.database")
                    .intType()
                    .defaultValue(0)
                    .withDescription("Optional database for connect to redis");

    public static final ConfigOption<String> HOST =
            ConfigOptions.key("client.host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("redis hostName");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("client.password")
                    .stringType()
                    .defaultValue("")
                    .withDescription("redis password");

    public static final ConfigOption<String> COMMAND =
            ConfigOptions.key("client.command")
                    .stringType()
                    .defaultValue("set")
                    .withDescription("redis command: set or hset");

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
        ReadableConfig config = helper.getOptions();

        validateFormat(config.get(FactoryUtil.FORMAT));
        validateCommand(config.get(COMMAND));
        validateModel(config.get(MODEL));
        validateTtl(config.get(COMMAND), config.get(SINK_VALUE_TTL));
        EncodingFormat<SerializationSchema<RowData>> format =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT);
        helper.validateExcept(LOOKUP_PREFIX);

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        ClientConfig clientConfig = getClientConfig(config);
        SinkConfig sinkConfig = getSinkConfig(config);

        return new LettuceRedisDynamicTableSink(resolvedSchema, clientConfig, sinkConfig, format);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // discover a suitable decoding format
        ReadableConfig config = helper.getOptions();

        validateFormat(config.get(FactoryUtil.FORMAT));
        validateCommand(config.get(COMMAND));
        validateModel(config.get(MODEL));
        DecodingFormat<DeserializationSchema<RowData>> format =
                helper.discoverDecodingFormat(
                        DeserializationFormatFactory.class, FactoryUtil.FORMAT);
        // validate  options except sink.* or json.* or csv.*
        helper.validateExcept(SINK_PREFIX);

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();

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
        options.add(HOST);
        options.add(COMMAND);
        options.add(FactoryUtil.FORMAT);
        options.add(MODEL);

        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DATABASE);
        options.add(PASSWORD);
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
        // split host to get hosts and ports
        String[] hostPorts = config.get(HOST).replace(" ", "").split(",");
        String[] hosts = Arrays.stream(hostPorts).map(s -> s.split(":")[0]).toArray(String[]::new);
        int[] ports =
                Arrays.stream(hostPorts).mapToInt(s -> Integer.parseInt(s.split(":")[1])).toArray();

        return new ClientConfig.Builder()
                .setHosts(hosts)
                .setPorts(ports)
                .setCommand(config.get(COMMAND))
                .setFormat(config.get(FactoryUtil.FORMAT))
                .setDatabase(config.get(DATABASE))
                .setPassword(config.get(PASSWORD))
                .setModel(config.get(MODEL))
                .build();
    }

    private LookUpConfig getLookUpConfig(ReadableConfig config) {
        return new LookUpConfig.Builder()
                .setCacheMaxRows(config.get(LOOKUP_CACHE_MAX_ROWS))
                .setCacheTtl(config.get(LOOKUP_CACHE_TTL))
                .build();
    }
}
