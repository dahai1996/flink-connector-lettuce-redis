package org.apache.flink.streaming.connectors.redis.table;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.embedded.RedisServer;

public class LookUpSQLTest {

    private static RedisServer redisServer;
    private static RedisClient redisClient;
    private static StatefulRedisConnection<String, String> connect;
    private static RedisCommands<String, String> sync;

    @BeforeClass
    public static void startup() {
        redisServer = RedisServer.builder().port(63792).setting("maxmemory 64m").build();

        redisServer.start();
        redisClient = RedisClient.create(RedisURI.create("127.0.0.1", 63792));
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
    public void testSetLookUpSQL() throws Exception {

        sync.set("1", "{\"id\":\"11\",\"info\":\"info1\",\"grand\":\"\"}");
        sync.set("2", "{\"id\":\"22\",\"info\":\"info2\",\"grand\":\"\",\"other\":\"other\"}");
        sync.set("3", "{\"id\":\"33\",\"info\":\"info3\"}");
        sync.set("4", "{\"info\":\"info4\",\"id\":\"44\"}");
        sync.set("5", "{}");
        sync.set("6", "{\"info\":\"info6\",\"id\":\"66\",\"grand\":\"grand6\"}");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddlWb =
                "create table dim_table(key string, id string,info string,grand string) with ( 'connector'='lettuce-redis', "
                        + "'hostname'='127.0.0.1','port'='63792','command'='set','format'='json','json.fail-on-missing-field'='false','json.map-null-key.mode'='LITERAL','json.map-null-key.literal'='HNULL')";

        String source =
                "create table source_table(username string, level string, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.username.kind'='sequence',  'fields.username.start'='1',  'fields.username.end'='9',"
                        + "'fields.level.kind'='sequence',  'fields.level.start'='1',  'fields.level.end'='9'"
                        + ")";

        String sink =
                "create table sink_table(username string, level string,dkey string,dname string,dinfo string,dgrand string) with ('connector'='print')";

        tEnv.executeSql(source);
        tEnv.executeSql(ddlWb);
        tEnv.executeSql(sink);

        String sql =
                " insert into sink_table "
                        + " select s.username, s.level, d.key ,d.id,d.info,d.grand from source_table  s"
                        + "  left join dim_table for system_time as of s.proctime as d "
                        + " on d.key = s.username";

        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
    }

    @Test
    public void testHsetLookUpSQL() throws Exception {
        sync.hset("test_dim", "1", "{\"id\":11,\"info\":\"info1\",\"grand\":\"grand1\"}");
        sync.hset("test_dim", "2", "{\"id\":22,\"info\":\"info2\",\"grand\":\"grand2\"}");
        sync.hset("test_dim", "3", "{\"id\":33,\"info\":\"info3\",\"grand\":\"grand3\"}");
        sync.hset("test_dim", "4", "{\"id\":44,\"info\":\"info4\",\"grand\":\"grand4\"}");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddlWb =
                "create table dim_table(hsetName String,fieldName string, id int,info string,grand string) with ( 'connector'='lettuce-redis', "
                        + "'hostname'='127.0.0.1','port'='63792','command'='hset','format'='json','json.fail-on-missing-field'='false','json.map-null-key.mode'='LITERAL','json.map-null-key.literal'='HNULL')";

        String source =
                "create table source_table(username string, level string, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.username.kind'='sequence',  'fields.username.start'='1',  'fields.username.end'='9',"
                        + "'fields.level.kind'='sequence',  'fields.level.start'='1',  'fields.level.end'='9'"
                        + ")";

        String sink =
                "create table sink_table(username string, level string,dhsetName string,dfieldName string,did int,dgrand string) with ('connector'='print')";

        tEnv.executeSql(source);
        tEnv.executeSql(ddlWb);
        tEnv.executeSql(sink);

        String sql =
                " insert into sink_table "
                        + " select s.username, s.level,d.hsetName,d.fieldName, d.id ,d.grand from source_table  s"
                        + "  left join dim_table for system_time as of s.proctime as d "
                        + " on  d.hsetName='test_dim' and d.fieldName = s.username ";

        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
    }

    @Test
    public void testSetLookUpSQLCsv() throws Exception {

        sync.set("1", "11,info1,grand1");
        sync.set("2", "22,,grand2");
        sync.set("3", "33,,");
        sync.set("4", ",,grand4");
        sync.set("5", "55,,grand5");
        sync.set("6", "66,info6,grand4");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddlWb =
                "create table dim_table(key string, id string,info string,grand string) with ( 'connector'='lettuce-redis', "
                        + "'hostname'='127.0.0.1','port'='63792','command'='set','format'='csv','csv.ignore-parse-errors'='true','csv.null-literal'='true')";

        String source =
                "create table source_table(username string, level string, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.username.kind'='sequence',  'fields.username.start'='1',  'fields.username.end'='9',"
                        + "'fields.level.kind'='sequence',  'fields.level.start'='1',  'fields.level.end'='9'"
                        + ")";

        String sink =
                "create table sink_table(username string, level string,dkey string,did string,dinfo string,dgrand string) with ('connector'='print')";

        tEnv.executeSql(source);
        tEnv.executeSql(ddlWb);
        tEnv.executeSql(sink);

        String sql =
                " insert into sink_table "
                        + " select s.username, s.level, d.key ,d.id,d.info,d.grand from source_table  s"
                        + "  left join dim_table for system_time as of s.proctime as d "
                        + " on d.key = s.username";

        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
    }

    @Test
    public void testHsetLookUpSQLCsv() throws Exception {
        sync.hset("test_dim", "1", "11,info1,grand1");
        sync.hset("test_dim", "2", "22,,grand1");
        sync.hset("test_dim", "3", "33,info1,");
        sync.hset("test_dim", "4", ",,");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddlWb =
                "create table dim_table(hsetName String,fieldName string, id int,info string,grand string) with ( 'connector'='lettuce-redis', "
                        + "'hostname'='127.0.0.1','port'='63792','command'='hset','format'='csv','csv.ignore-parse-errors'='true','csv.null-literal'='true')";

        String source =
                "create table source_table(username string, level string, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.username.kind'='sequence',  'fields.username.start'='1',  'fields.username.end'='9',"
                        + "'fields.level.kind'='sequence',  'fields.level.start'='1',  'fields.level.end'='9'"
                        + ")";

        String sink =
                "create table sink_table(username string, level string,dhsetName string,dfieldName string,did int,dinfo string,dgrand string) with ('connector'='print')";

        tEnv.executeSql(source);
        tEnv.executeSql(ddlWb);
        tEnv.executeSql(sink);

        String sql =
                " insert into sink_table "
                        + " select s.username, s.level,d.hsetName,d.fieldName, d.id ,d.info,d.grand from source_table  s"
                        + "  left join dim_table for system_time as of s.proctime as d "
                        + " on  d.hsetName='test_dim' and d.fieldName = s.username ";

        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
    }

    @Test
    public void testSql() throws Exception {
        sync.set("1", "{\"id\":\"11\",\"info\":\"info1\",\"grand\":\"\"}");
        sync.set("2", "{\"id\":\"22\",\"info\":\"info2\",\"grand\":\"\",\"other\":\"other\"}");
        sync.set("3", "{\"id\":\"33\",\"info\":\"info3\"}");
        sync.set("4", "{\"info\":\"info4\",\"id\":\"44\"}");
        sync.set("5", "{}");

        sync.hset("test_dim", "1", "{\"id\":111,\"info\":\"info1\",\"grand\":\"grand1\"}");
        sync.hset("test_dim", "2", "{\"id\":222,\"info\":\"info2\",\"grand\":\"grand2\"}");
        sync.hset("test_dim", "3", "{\"id\":333,\"info\":\"info3\",\"grand\":\"grand3\"}");
        sync.hset("test_dim", "4", "{\"id\":444,\"info\":\"info4\",\"grand\":\"grand4\"}");

        sync.hset("test_dim2", "1", "{\"id\":111,\"info\":\"info1\",\"grand\":\"grand1\"}");
        sync.hset("test_dim2", "2", "{\"id\":222,\"info\":\"info2\",\"grand\":\"grand2\"}");
        sync.hset("test_dim2", "3", "{\"id\":333,\"info\":\"info3\",\"grand\":\"grand3\"}");
        sync.hset("test_dim2", "4", "{\"id\":444,\"info\":\"info4\",\"grand\":\"grand4\"}");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddlWb =
                "create table dim_table(key string, id int,info string,grand string) with ( 'connector'='lettuce-redis', "
                        + "'hostname'='127.0.0.1','port'='63792','command'='set','format'='json','json.fail-on-missing-field'='false','json.map-null-key.mode'='LITERAL','json.map-null-key.literal'='HNULL')";

        String ddlWb2 =
                "create table dim_table2(hsetName String,fieldName string, id int,info string,grand string) with ( 'connector'='lettuce-redis', "
                        + "'hostname'='127.0.0.1','port'='63792','command'='hset','format'='json','json.fail-on-missing-field'='false','json.map-null-key.mode'='LITERAL','json.map-null-key.literal'='HNULL')";

        String source =
                "create table source_table(username string, level string, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.username.kind'='sequence',  'fields.username.start'='1',  'fields.username.end'='9',"
                        + "'fields.level.kind'='sequence',  'fields.level.start'='1',  'fields.level.end'='9'"
                        + ")";

        String sink =
                "create table sink_table(username string, level string,dkey string,did int,dinfo string,dgrand string) with ('connector'='print')";

        tEnv.executeSql(source);
        tEnv.executeSql(ddlWb);
        tEnv.executeSql(ddlWb2);
        tEnv.executeSql(sink);

        String sql =
                " insert into sink_table "
                        + " select s.username, s.level, d.key ,d.id,d.info,d.grand from source_table  s"
                        + "  left join dim_table for system_time as of s.proctime as d "
                        + " on d.key = s.username";
        String sql2 =
                " insert into sink_table "
                        + " select s.username, s.level,d.hsetName, d.id ,d.info,d.grand from source_table  s"
                        + "  left join dim_table2 for system_time as of s.proctime as d "
                        + " on  d.hsetName='test_dim' and d.fieldName = s.username ";

        String sql3 =
                " insert into sink_table "
                        + " select s.username, s.level,d.hsetName, d.id ,d.info,d.grand from source_table  s"
                        + "  left join dim_table2 for system_time as of s.proctime as d "
                        + " on  d.hsetName='test_dim2' and d.fieldName = s.username ";

        tEnv.executeSql(sql);
        tEnv.executeSql(sql2);
        TableResult tableResult3 = tEnv.executeSql(sql3);
        tableResult3.getJobClient().get().getJobExecutionResult().get();
    }
}
