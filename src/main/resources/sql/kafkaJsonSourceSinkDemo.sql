--sourceTable
CREATE TABLE user_log(
    user_id VARCHAR,
    item_id VARCHAR,
    category_id VARCHAR,
    behavior VARCHAR,
    ts TIMESTAMP(3)
) WITH (
    'connector.type' = 'kafka',
    'connector.version' = 'universal',
    'connector.topic' = 'user_behavior',
    'connector.properties.zookeeper.connect' = 'venn:2181',
    'connector.properties.bootstrap.servers' = 'venn:9092',
    'connector.startup-mode' = 'earliest-offset',
    'format.type' = 'json'
#    'format.type' = 'csv'
);

--sinkTable
CREATE TABLE user_log_sink (
    user_id VARCHAR,
    item_id VARCHAR,
    category_id VARCHAR,
    behavior VARCHAR,
    ts TIMESTAMP(3)
) WITH (
    'connector.type' = 'kafka',
    'connector.version' = 'universal',
    'connector.topic' = 'user_behavior_sink',
    'connector.properties.zookeeper.connect' = 'venn:2181',
    'connector.properties.bootstrap.servers' = 'venn:9092',
    'update-mode' = 'append',
#    'format.type' = 'json'
     'format.type' = 'csv'
);

--insert
INSERT INTO user_log_sink
SELECT user_id, item_id, category_id, behavior, ts
FROM user_log;
