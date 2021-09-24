package com.venn.source.cust;

import com.venn.util.HttpClientUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class CustHttpSource extends RichSourceFunction<String> {

    private String url;
    private long requestInterval;
    private boolean flag = false;
    private transient Counter counter;

    public CustHttpSource(String url, long requestInterval) {
        this.url = url;
        this.requestInterval = requestInterval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        flag = true;

        counter = new SimpleCounter();
        this.counter = getRuntimeContext()
                .getMetricGroup()
                .counter("myCounter");

    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {


        while (true) {
            String result = HttpClientUtil.doGet(url);

            ctx.collect(result);
            this.counter.inc();

            Thread.sleep(requestInterval);
        }

    }

    @Override
    public void cancel() {
        flag = false;
    }
}
