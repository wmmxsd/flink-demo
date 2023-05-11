package com.wmm.flink.processfunction;

import com.wmm.flink.common.UrlViewCount;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;

public class TopN extends KeyedProcessFunction<Long, UrlViewCount, String> {
    /**
     * 将 n 作为属性
     */
    private Integer n;
    /**
     * 定义一个列表状态
     */
    private ListState<UrlViewCount> urlViewCountListState;

    public TopN(Integer n) {
        this.n = n;
    }

    @Override
    public void open(Configuration parameters) {

        // 从环境中获取列表状态句柄
        urlViewCountListState = getRuntimeContext().getListState(

                new ListStateDescriptor<>("url-view-count-list", Types.POJO(UrlViewCount.class)));
    }

    @Override
    public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
        // 将 count 数据添加到列表状态中，保存起来
        urlViewCountListState.add(value);
        // 注册 window end + 1ms 后的定时器，等待所有数据到齐开始排序
        ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 将数据从列表状态变量中取出，放入 ArrayList，方便排序
        ArrayList<UrlViewCount> urlViewCountArrayList = new ArrayList<>();
        for (UrlViewCount urlViewCount : urlViewCountListState.get()) {
            urlViewCountArrayList.add(urlViewCount);
        }
        // 清空状态，释放资源
        urlViewCountListState.clear();
        // 排序
        urlViewCountArrayList.sort((o1, o2) -> o2.count.intValue() - o1.count.intValue());
        // 取前两名，构建输出结果
        StringBuilder result = new StringBuilder();
        result.append("========================================\n");
        result.append("窗口结束时间：" + new Timestamp(timestamp - 1) + "\n");
        for (int i = 0; i < this.n; i++) {
            UrlViewCount urlViewCount = urlViewCountArrayList.get(i);
            String info = "No." + (i + 1) + " "
                    + "url：" + urlViewCount.url + " "
                    + "浏览量：" + urlViewCount.count + "\n";
            result.append(info);
        }
        result.append("========================================\n");
        out.collect(result.toString());
    }

    public Integer getN() {
        return n;
    }

    public void setN(Integer n) {
        this.n = n;
    }

    public ListState<UrlViewCount> getUrlViewCountListState() {
        return urlViewCountListState;
    }

    public void setUrlViewCountListState(ListState<UrlViewCount> urlViewCountListState) {
        this.urlViewCountListState = urlViewCountListState;
    }
}
