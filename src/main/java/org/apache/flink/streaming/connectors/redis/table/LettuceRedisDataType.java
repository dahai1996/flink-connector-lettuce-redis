package org.apache.flink.streaming.connectors.redis.table;

/**
 * @author sqh
 */
public enum LettuceRedisDataType {
    // set
    SET("set"),
    // hset
    HSET("hset");

    String name;

    LettuceRedisDataType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
