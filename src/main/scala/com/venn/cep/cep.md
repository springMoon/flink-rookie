# CEP Demo
```text
this package for cep demo
```

## References
```text
官网文档：https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/libs/cep.html
官网文档翻译：https://www.cnblogs.com/Springmoon-venn/p/11993468.html
刘博 Flink CEP 实战: 
    PPT : https://files.alicdn.com/tpsservice/94d409d9679d1b46034f7d00161d99a7.pdf
    视频 ： https://www.bilibili.com/video/av66073054/
刘博 Apache Flink CEP 实战 ： https://mp.weixin.qq.com/s/4dQYr-RXKBRdrhu6Y5dZdw
Flink-CEPplus 项目：https://github.com/ljygz/Flink-CEPplus (作者和 末日布孤单 应该是一个人)
末日布孤单源 CEP 码解析：https://www.cnblogs.com/ljygz/p/11978386.html
```

## 匹配后跳过策略：
```text
模式： b+ c
input : b1 b2 b3 c
NO_SKIP ： b1 b2 b3 c / b2 b3 c / b3 c # 一次只跳过一个事件，就开始匹配
SKIP_TO_NEXT : b1 b2 b3 c / b2 b3 c / b3 c # 调到下一个 开始事件（也就是 b）
SKIP_PAST_LAST_EVENT : b1 b2 b3 c # 跳过所有匹配过的事件
SKIP_TO_FIRST[b] : b1 b2 b3 c / b2 b3 c / b3 c # 跳到第一个b（如果 第一个就是 b，从这个 b 后面的第一个b 开始）？
SKIP_TO_LAST[b] : b1 b2 b3 c / b3 c # 跳到最后一个b，如果模式里面没有连续的b， 应该是调到 c 的后一个事件 ？
```