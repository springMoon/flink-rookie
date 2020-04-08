-- 读 json，写csv
---sourceTable
CREATE TABLE user_log(
    user_id VARCHAR,
    item_id VARCHAR,
    category_id VARCHAR,
    behavior VARCHAR,
    ts TIMESTAMP(3),
    proctime as PROCTIME()
) WITH (
    'connector.type' = 'kafka',
    'connector.version' = 'universal',
    'connector.topic' = 'user_behavior',
    'connector.properties.zookeeper.connect' = 'venn:2181',
    'connector.properties.bootstrap.servers' = 'venn:9092',
    'connector.startup-mode' = 'earliest-offset',
    'format.type' = 'json'
);

---sinkTable
CREATE TABLE user_log_sink (
    item_id VARCHAR ,
    category_id VARCHAR ,
    behavior VARCHAR ,
    max_tx TIMESTAMP(3),
    min_prc TIMESTAMP(3),
    max_prc TIMESTAMP(3),
    coun BIGINT
) WITH (
    'connector.type' = 'myKafka',
    'connector.version' = 'universal',
    'connector.topic' = 'user_behavior_sink',
    'connector.properties.zookeeper.connect' = 'venn:2181',
    'connector.properties.bootstrap.servers' = 'venn:9092',
    'update-mode' = 'upsert',
    'format.type' = 'json'
);

---insert
INSERT INTO user_log_sink
SELECT item_id, category_id, behavior, max(ts), min(proctime), max(proctime), count(user_id)
FROM user_log
group by item_id, category_id, behavior;
-- SELECT item_id, category_id, behavior, max(ts), min(proctime), max(proctime), count(user_id)
-- from user_log
-- group by TUMBLE(proctime, INTERVAL '1' MINUTE ), item_id,category_id,behavior;