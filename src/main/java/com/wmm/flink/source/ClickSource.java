package com.wmm.flink.source;

import com.wmm.flink.transform.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Clock;
import java.util.Random;

/**
 * 自定义数据源
 */
public class ClickSource implements SourceFunction<Event> {
    private boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
        while (running) {
            ctx.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Clock.systemUTC().millis()
            ));
            //隔 1 秒生成一个点击事件，方便观测
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
