package com.yewenxin;


import com.yewenxin.pojo.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {
    public boolean running=true;
    Random random = new Random();
    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        String[] users = {"Alice","Bob","Kevin","Mary"};
        String[] urls = {"./home","./addCurt","./prods?id=12","./pords?id=36","./view"};

        while(running){
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Long timeStamp = Calendar.getInstance().getTimeInMillis();
            sourceContext.collect(new Event(user,url,timeStamp));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running=false;
    }
}
