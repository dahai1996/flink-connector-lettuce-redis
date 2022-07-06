package org.apache.flink.streaming.connectors.redis.table;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;

public class ClusterLookUpSQLTest {

    static int[] ports = {6381, 6383, 6384, 6385, 6386};
    static String[] hosts = {"node", "node", "node", "node", "node"};
    private static RedisClusterClient redisClusterClient;
    private static StatefulRedisClusterConnection<String, String> connect1;
    private static RedisAdvancedClusterCommands<String, String> sync;

    @BeforeClass
    public static void startup() {

        ArrayList<RedisURI> urlList = new ArrayList<>();
        for (int i = 0; i < hosts.length; i++) {
            RedisURI uri =
                    RedisURI.Builder.redis(hosts[i], ports[i])
                            .withDatabase(0)
                            .withPassword("123456".toCharArray())
                            .build();
            urlList.add(uri);
        }

        redisClusterClient = RedisClusterClient.create(urlList);
        connect1 = redisClusterClient.connect();
        sync = connect1.sync();
    }

    @AfterClass
    public static void end() {
        connect1.close();
        redisClusterClient.shutdown();
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
        env.setParallelism(1);

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddlWb =
                "create table dim_table(key string, id string,info string,grand string) with ( 'connector'='lettuce-redis', "
                        + "'client.host'='node:6381,node:6382,node:6383,node:6384,node:6385,node:6386',"
                        + "'client.model'='cluster','client.password'='123456','client.database'='0',"
                        + "'client.command'='set','format'='json','json.fail-on-missing-field'='false','json.map-null-key.mode'='LITERAL','json.map-null-key.literal'='HNULL')";

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
    public void insertSinkTest() throws Exception {
        // set json

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl =
                "create table sink_redis(setName VARCHAR, v1 String,v2 String) with ( 'connector'='lettuce-redis', "
                        + "'client.host'='node:6382',"
                        + "'client.model'='cluster','client.password'='123456','client.database'='0',"
                        + "'client.command'='set','format'='json','client.database'='1',"
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
}
