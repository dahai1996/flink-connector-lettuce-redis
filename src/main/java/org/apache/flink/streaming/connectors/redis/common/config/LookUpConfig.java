package org.apache.flink.streaming.connectors.redis.common.config;

import java.io.Serializable;

/**
 * @author sqh
 */
public class LookUpConfig implements Serializable {
    long cacheMaxRows;
    long cacheTtl;

    public LookUpConfig(long cacheMaxRows, long cacheTtl) {
        this.cacheMaxRows = cacheMaxRows;
        this.cacheTtl = cacheTtl;
    }

    public long getCacheMaxRows() {
        return cacheMaxRows;
    }

    public long getCacheTtl() {
        return cacheTtl;
    }

    public static class Builder {
        long cacheMaxRows;
        long cacheTtl;

        public Builder setCacheMaxRows(long cacheMaxRows) {
            this.cacheMaxRows = cacheMaxRows;
            return this;
        }

        public Builder setCacheTtl(long cacheTtl) {
            this.cacheTtl = cacheTtl;
            return this;
        }

        public LookUpConfig build() {
            return new LookUpConfig(cacheMaxRows, cacheTtl);
        }
    }
}
