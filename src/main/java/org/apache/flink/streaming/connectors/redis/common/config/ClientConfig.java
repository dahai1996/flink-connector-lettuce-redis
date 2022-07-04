package org.apache.flink.streaming.connectors.redis.common.config;

import java.io.Serializable;

/**
 * @author sqh
 */
public class ClientConfig implements Serializable {
    String hostname;
    String command;
    int port;
    String format;

    int database;

    public ClientConfig(String hostname, String command, int port, String format, int database) {
        this.hostname = hostname;
        this.command = command;
        this.port = port;
        this.format = format;
        this.database = database;
    }

    public String getHostname() {
        return hostname;
    }

    public String getCommand() {
        return command;
    }

    public int getPort() {
        return port;
    }

    public String getFormat() {
        return format;
    }

    public int getDatabase() {
        return database;
    }

    public static class Builder {
        String hostname;
        String command;
        int port;
        String format;
        int database;

        public Builder setHostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder setCommand(String command) {
            this.command = command;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setFormat(String format) {
            this.format = format;
            return this;
        }

        public Builder setDatabase(int database) {
            this.database = database;
            return this;
        }

        public ClientConfig build() {
            return new ClientConfig(hostname, command, port, format, database);
        }
    }
}
