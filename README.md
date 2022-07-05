# flink-connector-lettuce-redis

flink connect for redis

use luttuce to connect to redis

## todo:
 - support redis-cluster
 - support other redis commands
 - support streaming api

## dependency:

| name    | version     | desc                          |
|---------|-------------|-------------------------------|
| flink   | 1.13.2-2.12 | stream table                  |
| lettuce | 6.1.8.RELEASE | only support single redis now |

## supported : lookUp ,sink

| Name  | Version | Source        | Sink                 |
|-------|---------|---------------|----------------------|
| flink-connector-lettuce-redis | 1.0.0   | Lookup(async) | Streaming Sink(sync) |

## parameter:

| parameter name | type   | default | desc                                                                                                                                              |
|--------------|--------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------|
|     hostname | string |         | redis host:only support single redis now                                                                                                          |
|     port         | int    | 6379    | redis port                                                                                                                                        |
| database | int    | 0       | redis database                                                                                                                                    |
|command | string | set     | redis command: only support set,hset                                                                                                              |
|lookup.cache.max-rows|long| -1      | max rows to cache in lookup, -1 means no cache                                                                                                    |
| lookup.cache.ttl|long| -1      | ttl for cache in lookup, -1 means no cache                                                                                                        |
|lookup.max-retries|int| 1       | lookup max retries                                                                                                                                |
|sink.value.ttl|int| 0       | sink redis ttl time,only support when command is 'set',default 0 means no expire time                                                             |
|sink.max-retries|int| 1       | sink max retries                                                                                                                                  |
|sink.parallelism|int| 1       | sink parallelism                                                                                                                                  |
|format|string|         | only support format : json,csv                                                                                                                    |
|json.*|string|         | when format is json,can set [Json Format Options](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/table/formats/json/) |
|csv.*|string| | when format is csv,can set [Csv Format Options](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/table/formats/csv/)    |

## examples:

to see more
examples,visit [flink-connector-lettuce-redis-test](https://github.com/dahai1996/-flink-connector-lettuce-redis/tree/main/src/test/java/org/apache/flink/streaming/connectors/redis/table)

### look up:

```
    // set some key-value to redis
    sync.set("1","{\"id\":\"11\",\"info\":\"info1\",\"grand\":\"\"}");
    sync.set("2","{\"id\":\"22\",\"info\":\"info2\",\"grand\":\"\",\"other\":\"other\"}");
    sync.set("3","{\"id\":\"33\",\"info\":\"info3\"}");
    sync.set("4","{\"info\":\"info4\",\"id\":\"44\"}");
    sync.set("5","{}");
    sync.set("6","{\"info\":\"info6\",\"id\":\"66\",\"grand\":\"grand6\"}");
```

```
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings=
        EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv=StreamTableEnvironment.create(env,environmentSettings);

        String ddlWb=
        "create table dim_table(key string, id string,info string,grand string) with ( 'connector'='lettuce-redis', "
        +"'hostname'='127.0.0.1','port'='63792','command'='set','format'='json','json.fail-on-missing-field'='false','json.map-null-key.mode'='LITERAL','json.map-null-key.literal'='HNULL')";

        String source=
        "create table source_table(username string, level string, proctime as procTime()) "
        +"with ('connector'='datagen',  'rows-per-second'='1', "
        +"'fields.username.kind'='sequence',  'fields.username.start'='1',  'fields.username.end'='9',"
        +"'fields.level.kind'='sequence',  'fields.level.start'='1',  'fields.level.end'='9'"
        +")";

        String sink=
        "create table sink_table(username string, level string,dkey string,dname string,dinfo string,dgrand string) with ('connector'='print')";

        tEnv.executeSql(source);
        tEnv.executeSql(ddlWb);
        tEnv.executeSql(sink);

        String sql=
        " insert into sink_table "
        +" select s.username, s.level, d.key ,d.id,d.info,d.grand from source_table  s"
        +"  left join dim_table for system_time as of s.proctime as d "
        +" on d.key = s.username";

        TableResult tableResult=tEnv.executeSql(sql);
```

we get the result:

```shell
+I[8, 8, null, null, null, null]
+I[7, 7, null, null, null, null]
+I[5, 5, 5, null, null, null]
+I[2, 2, 2, 22, info2, ]
+I[6, 6, 6, 66, info6, grand6]
+I[1, 1, 1, 11, info1, ]
+I[4, 4, 4, 44, info4, null]
+I[3, 3, 3, 33, info3, null]
+I[9, 9, null, null, null, null]
```

### sink:

```
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings=
        EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv=StreamTableEnvironment.create(env,environmentSettings);

        String ddl=
        "create table sink_redis(setName VARCHAR, v1 String,v2 String) with ( 'connector'='lettuce-redis', "
        +"'hostname'='127.0.0.1','port'='63792','command'='set','format'='json','database'='1',"
        +"'json.fail-on-missing-field'='true')";

        tEnv.executeSql(ddl);
        String sql=" insert into sink_redis select * from (values ('setName1', 'v1','v2'))";
        String sql2=" insert into sink_redis select * from (values ('setName2', 'v21','v22'))";
        String sql3=" insert into sink_redis select * from (values ('setName3', 'v31','v32'))";
        tEnv.executeSql(sql);
        tEnv.executeSql(sql2);
        TableResult tableResult=tEnv.executeSql(sql3);
        tableResult.getJobClient().get().getJobExecutionResult().get();

        Assert.assertEquals("{\"v1\":\"v1\",\"v2\":\"v2\"}",sync.get("setName1"));
        Assert.assertEquals("{\"v1\":\"v21\",\"v2\":\"v22\"}",sync.get("setName2"));
        Assert.assertEquals("{\"v1\":\"v31\",\"v2\":\"v32\"}",sync.get("setName3"));
```