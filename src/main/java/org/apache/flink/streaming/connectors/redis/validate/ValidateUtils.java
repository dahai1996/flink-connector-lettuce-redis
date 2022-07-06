package org.apache.flink.streaming.connectors.redis.validate;

import org.apache.flink.streaming.connectors.redis.table.LettuceRedisDataType;
import org.apache.flink.table.api.ValidationException;

import java.util.HashSet;
import java.util.Set;

/**
 * @author sqh
 */
public class ValidateUtils {

    public static Set<String> supportFormatOptions() {
        final Set<String> options = new HashSet<>();
        options.add("json");
        options.add("csv");

        return options;
    }

    public static Set<String> supportCommandOptions() {
        final Set<String> options = new HashSet<>();
        options.add(LettuceRedisDataType.SET.getName());
        options.add(LettuceRedisDataType.HSET.getName());

        return options;
    }

    public static Set<String> supportModelOptions() {
        final Set<String> options = new HashSet<>();
        options.add("single");
        options.add("cluster");

        return options;
    }

    public static void validateTtl(String commandString, Integer integer) {
        // hset do not support expire
        if (integer != 0 && LettuceRedisDataType.HSET.getName().equals(commandString)) {
            throw new ValidationException(
                    "when command is 'hset',do not support 'sink.value.ttl'.You can set 'sink.value.ttl' to 0,it means no expire time");
        }
    }

    public static void validateCommand(String commandString) {
        if (!supportCommandOptions().contains(commandString)) {
            throw new ValidationException(
                    String.format(
                            "do not support this command : %s ,only support command : set,hset.",
                            commandString));
        }
    }

    public static void validateFormat(String formatConfig) {
        if (!supportFormatOptions().contains(formatConfig)) {
            throw new ValidationException(
                    String.format(
                            "do not support this format : %s ,only support format : json,csv.",
                            formatConfig));
        }
    }

    public static void validateModel(String model) {
        if (!supportModelOptions().contains(model)) {
            throw new ValidationException(
                    String.format(
                            "do not support this model : %s ,only support model : single,cluster.",
                            model));
        }
    }
}
