package org.apache.flink.streaming.connectors.redis.table;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.RedisURI.Builder;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.embedded.RedisServer;

public class SinkSQLTest {
    private static RedisServer redisServer;
    private static RedisClient redisClient;
    private static StatefulRedisConnection<String, String> connect;
    private static RedisCommands<String, String> sync;

    @BeforeClass
    public static void startup() {
        redisServer = RedisServer.builder().port(63792).setting("maxmemory 64m").build();

        redisServer.start();
        RedisURI build = Builder.redis("127.0.0.1", 63792).withDatabase(1).build();
        redisClient = RedisClient.create(build);
        connect = redisClient.connect();
        sync = connect.sync();
    }

    @AfterClass
    public static void end() {
        connect.close();
        redisClient.shutdown();
        redisServer.stop();
    }

    @Test
    public void lettuceClientTest() {
        RedisURI redisURI =
                RedisURI.builder()
                        .withDatabase(0)
                        .withHost("127.0.0.1")
                        .withPort(63792)
                        .withSsl(false)
                        .withTimeout(Duration.ofSeconds(100))
                        .build();

        //        RedisClusterClient redisClusterClient = RedisClusterClient.create(redisURI);
        //        StatefulRedisClusterConnection<String, String> connect1 =
        // redisClusterClient.connect();
        //        RedisAdvancedClusterCommands<String, String> sync1 = connect1.sync();

        RedisClient redisClient1 = RedisClient.create(redisURI);
        StatefulRedisConnection<String, String> connect1 = redisClient1.connect();
        RedisCommands<String, String> sync1 = connect1.sync();

        sync1.set("test1", "value1");
        System.out.println(sync1.get("test1"));
    }

    @Test
    public void insertSinkTest() throws Exception {
        // set json

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl =
                "create table sink_redis(setName VARCHAR, v1 String,v2 String) with ( 'connector'='lettuce-redis', "
                        + "'hostname'='127.0.0.1','port'='63792','command'='set','format'='json','database'='1',"
                        + "'json.fail-on-missing-field'='true')";

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('setName1', 'v1','v2'))";
        String sql2 = " insert into sink_redis select * from (values ('setName2', 'v21','v22'))";
        String sql3 = " insert into sink_redis select * from (values ('setName3', 'v31','v32'))";
        tEnv.executeSql(sql);
        tEnv.executeSql(sql2);
        TableResult tableResult = tEnv.executeSql(sql3);
        tableResult.getJobClient().get().getJobExecutionResult().get();

        Assert.assertEquals("{\"v1\":\"v1\",\"v2\":\"v2\"}", sync.get("setName1"));
        Assert.assertEquals("{\"v1\":\"v21\",\"v2\":\"v22\"}", sync.get("setName2"));
        Assert.assertEquals("{\"v1\":\"v31\",\"v2\":\"v32\"}", sync.get("setName3"));
    }

    @Test
    public void insertCsvSinkTest() throws Exception {
        // set csv

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl =
                "create table sink_redis(setName VARCHAR, v1 String,v2 String) with ( 'connector'='lettuce-redis', "
                        + "'hostname'='127.0.0.1','port'='63792','command'='set','format'='csv','database'='1')";

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('setName1', 'v1','v2'))";
        String sql2 = " insert into sink_redis select * from (values ('setName2', 'v21','v22'))";
        String sql3 = " insert into sink_redis select * from (values ('setName3', 'v31','v32'))";
        tEnv.executeSql(sql);
        tEnv.executeSql(sql2);
        TableResult tableResult = tEnv.executeSql(sql3);
        tableResult.getJobClient().get().getJobExecutionResult().get();

        Assert.assertEquals("v1,v2", sync.get("setName1"));
        Assert.assertEquals("v21,v22", sync.get("setName2"));
        Assert.assertEquals("v31,v32", sync.get("setName3"));
    }

    @Test
    public void hsetInsertSqlTest() throws Exception {
        // hset json

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl =
                "create table sink_redis(keyName string,fieldName string, v1 String,v2 String,v3 String) with ( 'connector'='lettuce-redis', "
                        + "'hostname'='127.0.0.1','port'='63792','command'='hset','format'='json','database'='1',"
                        + "'json.fail-on-missing-field'='true')";

        tEnv.executeSql(ddl);
        String sql =
                " insert into sink_redis select * from (values ('key1','fieldName1', 'v1','v2','v3'))";
        String sql2 =
                " insert into sink_redis select * from (values ('key1','fieldName2', 'v21','v22','v32'))";
        String sql3 =
                " insert into sink_redis select * from (values ('key1','fieldName3', 'v31','v32','v33'))";
        tEnv.executeSql(sql);
        tEnv.executeSql(sql2);
        TableResult tableResult = tEnv.executeSql(sql3);
        tableResult.getJobClient().get().getJobExecutionResult().get();

        Assert.assertEquals(
                "{\"v1\":\"v1\",\"v2\":\"v2\",\"v3\":\"v3\"}", sync.hget("key1", "fieldName1"));
        Assert.assertEquals(
                "{\"v1\":\"v21\",\"v2\":\"v22\",\"v3\":\"v32\"}", sync.hget("key1", "fieldName2"));
        Assert.assertEquals(
                "{\"v1\":\"v31\",\"v2\":\"v32\",\"v3\":\"v33\"}", sync.hget("key1", "fieldName3"));
    }

    @Test
    public void hsetCsvInsertSqlTest() throws Exception {
        // hset csv

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl =
                "create table sink_redis(keyName string,fieldName string, v1 String,v2 String,msg String) with ( 'connector'='lettuce-redis', "
                        + "'hostname'='127.0.0.1','port'='63792','command'='hset','format'='csv','database'='1')";

        tEnv.executeSql(ddl);
        String sql =
                " insert into sink_redis select * from (values ('key1','fieldName1', 'v1','v2','11'))";
        String sql2 =
                " insert into sink_redis select * from (values ('key1','fieldName2', 'v21','v22','22'))";
        String sql3 =
                " insert into sink_redis select * from (values ('key1','fieldName3', 'v31','v32','33'))";
        tEnv.executeSql(sql);
        tEnv.executeSql(sql2);
        TableResult tableResult = tEnv.executeSql(sql3);
        tableResult.getJobClient().get().getJobExecutionResult().get();

        Assert.assertEquals("v1,v2,11", sync.hget("key1", "fieldName1"));
        Assert.assertEquals("v21,v22,22", sync.hget("key1", "fieldName2"));
        Assert.assertEquals("v31,v32,33", sync.hget("key1", "fieldName3"));
    }
}
