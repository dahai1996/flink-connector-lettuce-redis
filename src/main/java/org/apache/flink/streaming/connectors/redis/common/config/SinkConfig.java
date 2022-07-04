package org.apache.flink.streaming.connectors.redis.common.config;

import java.io.Serializable;

/**
 * @author sqh
 */
public class SinkConfig implements Serializable {

    int maxRetryTimes;
    int valueTtl;
    int parallelism;

    public SinkConfig(int maxRetryTimes, int valueTtl, int parallelism) {
        this.maxRetryTimes = maxRetryTimes;
        this.valueTtl = valueTtl;
        this.parallelism = parallelism;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public int getValueTtl() {
        return valueTtl;
    }

    public int getParallelism() {
        return parallelism;
    }

    public static class Builder {
        int maxRetryTimes;
        int valueTtl;
        int parallelism;

        public Builder setMaxRetryTimes(int maxRetryTimes) {
            this.maxRetryTimes = maxRetryTimes;
            return this;
        }

        public Builder setValueTtl(int valueTtl) {
            this.valueTtl = valueTtl;
            return this;
        }

        public Builder setParallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public SinkConfig build() {
            return new SinkConfig(maxRetryTimes, valueTtl, parallelism);
        }
    }
}
