package org.apache.flink.streaming.connectors.redis.config;

import java.io.Serializable;

/**
 * @author sqh
 */
public class ClientConfig implements Serializable {
    String[] hosts;
    int[] ports;
    String command;
    String format;
    String model;
    int database;
    String password;

    public ClientConfig(
            String[] hosts,
            int[] ports,
            String command,
            String format,
            String model,
            int database,
            String password) {
        this.hosts = hosts;
        this.ports = ports;
        this.command = command;
        this.format = format;
        this.model = model;
        this.database = database;
        this.password = password;
    }

    public String[] getHosts() {
        return hosts;
    }

    public int[] getPorts() {
        return ports;
    }

    public String getCommand() {
        return command;
    }

    public String getFormat() {
        return format;
    }

    public int getDatabase() {
        return database;
    }

    public String getModel() {
        return model;
    }

    public String getPassword() {
        return password;
    }

    public static class Builder {
        String[] hosts;
        int[] ports;
        String command;
        String format;
        String model;
        String password;
        int database;

        public Builder setHosts(String[] hosts) {
            this.hosts = hosts;
            return this;
        }

        public Builder setPorts(int[] ports) {
            this.ports = ports;
            return this;
        }

        public Builder setCommand(String command) {
            this.command = command;
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

        public Builder setModel(String model) {
            this.model = model;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public ClientConfig build() {
            return new ClientConfig(hosts, ports, command, format, model, database, password);
        }
    }
}
