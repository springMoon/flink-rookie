# Dynamic Window Staticstics


用 Flink 实现一个动态窗口统计的功能，使用 flink 1.10.0。实现的功能包括：


## 1. 定义命令流Source，格式
{
    'taskId':  '任务id'，
    'targetAttr': '要统计的属性',
    'method': '统计方法，有 SUM 求和，MAX 最大值， MIN 最小值三种'
    'periodUnit': '统计周期任务，有 SECOND 和 MINUTE 两个值',
    'periodLength': '周期的长度，数值',
    'startTime': '任务开始的UNIX时间戳，单位毫秒'
}


如:
{
    'taskId':  'task1'，
    'targetAttr': 'attr1',
    'method': 'SUM'
    'periodUnit': 'MINUTE',
    'periodLength': '3',
    'startTime': '1598596980000'
}
表示 从 2020/8/28 14:43:00 开始统计属性attr1每三分钟的和


题目要求命令流发送4条数据，固定为下：
{
    'taskId':  'task1'，
    'targetAttr': 'attr1',
    'method': 'SUM'
    'periodUnit': 'SECOND',
    'periodLength': '30',
    'startTime': '1598596980000'
}


{
    'taskId':  'task2'，
    'targetAttr': 'attr1',
    'method': 'SUM'
    'periodUnit': 'MINUTE',
    'periodLength': '1',
    'startTime': '1598596980000'
}


{
    'taskId':  'task3'，
    'targetAttr': 'attr2',
    'method': 'MAX'
    'periodUnit': 'SECOND',
    'periodLength': '30',
    'startTime': '1598596980000'
}


{
    'taskId':  'task4'，
    'targetAttr': 'attr3',
    'method': 'MAX'
    'periodUnit': 'MINUTE',
    'periodLength': '2',
    'startTime': '1598596980000'
}

```text
{"taskId":"task1","targetAttr":"attr2","method":"sum","periodUnit":"SECOND","periodLength":"20","startTime":"1598596980000"}
{"taskId":"task2","targetAttr":"attr1","method":"sum","periodUnit":"MINUTE","periodLength":"1","startTime":"1598596980000"}
{"taskId":"task3","targetAttr":"attr2","method":"max","periodUnit":"SECOND","periodLength":"30","startTime":"1598596980000"}
{"taskId":"task4","targetAttr":"attr3","method":"min","periodUnit":"MINUTE","periodLength":"1","startTime":"1599640669628"}

```


## 2. 定义数据流Source，格式如下：
{
    "attr": '属性名',
    "value": double数值,
    "time": 'UNIX时间戳，单位毫秒'
}

如:
{
    'attr': 'attr1',
    'value': 35.0,
    'time': '1598596980000'
}


数据流需要每一秒发送4条数据，属性分别是 attr1、attr2、attr3 和 attr4，time使用当前unix毫秒时间戳，value使用 0~100的随机整数


## 3. 需要将命令流进行广播，然后和数据流进行connect，根据命令流指定的命令进行统计

统计参考涉及到的部分类或方法： DataStream.assignTimestampsAndWatermarks、keyBy、WindowAssigner、reduce、ProcessWindowFunction、addSink   


## 4. 实现一个输出到终端的 sink，将统计结果打印出来，每一条记录包括 taskId, targetAttr, periodStartTime(周期开始时间), value (统计后的值，double类型)