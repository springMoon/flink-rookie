package com.venn.question.cdcStarrocks;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CdcStarProcessFunction extends KeyedProcessFunction<String, CdcRecord, List<CdcRecord>> {

    private final static Logger LOG = LoggerFactory.getLogger(CdcStarProcessFunction.class);
    private int batchSize;
    private long batchInterval;
    // next timer time
    private ValueState<Long> cacheTimer;
    // current cache size
    private ValueState<Integer> cacheSize;
    // cache data
    private ListState<CdcRecord> cache;

    public CdcStarProcessFunction(int batchSize, long batchInterval) {
        this.batchSize = batchSize;
        this.batchInterval = batchInterval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        ListStateDescriptor cacheDescriptor = new ListStateDescriptor<CdcRecord>("cache", TypeInformation.of(CdcRecord.class));
        cache = getRuntimeContext().getListState(cacheDescriptor);

        ValueStateDescriptor cacheSizeDescriptor = new ValueStateDescriptor<Integer>("cacheSize", Integer.class);
        cacheSize = getRuntimeContext().getState(cacheSizeDescriptor);

        ValueStateDescriptor cacheTimerDescriptor = new ValueStateDescriptor<Long>("cacheTimer", Long.class);
        cacheTimer = getRuntimeContext().getState(cacheTimerDescriptor);
    }

    @Override
    public void processElement(CdcRecord element, KeyedProcessFunction<String, CdcRecord, List<CdcRecord>>.Context ctx, Collector<List<CdcRecord>> out) throws Exception {

        // cache size + 1
        if (cacheSize.value() != null) {
            cacheSize.update(cacheSize.value() + 1);
        } else {
            cacheSize.update(1);
            // add timer for interval flush
            long nextTimer = System.currentTimeMillis() + batchInterval;
            LOG.debug("register timer : {} , key : {}", nextTimer, ctx.getCurrentKey());
            cacheTimer.update(nextTimer);
            ctx.timerService().registerProcessingTimeTimer(nextTimer);
        }
        // add data to cache state
        cache.add(element);
        // cache size max than batch Size
        if (cacheSize.value() >= batchSize) {
            // remove next timer
            long nextTimer = cacheTimer.value();
            LOG.debug("{} remove timer, key : {}", nextTimer, ctx.getCurrentKey());
            ctx.timerService().deleteProcessingTimeTimer(nextTimer);
            // flush data to down stream
            flushData(out);
        }
    }

    /**
     * flush data to down stream
     */
    private void flushData(Collector<List<CdcRecord>> out) throws Exception {
        List<CdcRecord> tmpCache = new ArrayList<>();
        Iterator<CdcRecord> it = cache.get().iterator();
        while (it.hasNext()) {
            tmpCache.add(it.next());
        }
        if (tmpCache.size() > 0) {
            out.collect(tmpCache);

            // finish flush all cache data, clear state
            cache.clear();
            cacheSize.clear();
            cacheTimer.clear();
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, CdcRecord, List<CdcRecord>>.OnTimerContext ctx, Collector<List<CdcRecord>> out) throws Exception {
        LOG.info("{} trigger timer to flush data", ctx.getCurrentKey(), timestamp);
        // batch interval trigger flush data
        flushData(out);
    }

    @Override
    public void close() throws Exception {
    }
}
