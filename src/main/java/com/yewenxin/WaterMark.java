package com.yewenxin;

import com.yewenxin.pojo.Event;
import com.yewenxin.pojo.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WaterMark {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        DataStreamSource<Event> dataStreamSource = env.addSource(new ClickSource());
        dataStreamSource.print("source");

        SingleOutputStreamOperator<UrlViewCount> aggregate = dataStreamSource.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        //水位线生成器
                        .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        //时间戳提取器
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timeStamp;
                            }
                        })
        ).keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new MyAggregateFunction(), new MyProcessWindowFunction());

        aggregate.keyBy(data->data.endWindowTime)
                .process(new MyProcessFunction());



        env.execute();
    }
    public static class MyProcessFunction extends KeyedProcessFunction<Long,UrlViewCount,String>{

        @Override
        public void processElement(UrlViewCount urlViewCount, Context context, Collector<String> collector) throws Exception {


        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

        }
    }

    public static class MyAggregateFunction implements AggregateFunction<Event,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event event, Long accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }
    public static class MyProcessWindowFunction extends ProcessWindowFunction<Long, UrlViewCount,String, TimeWindow>{

        @Override
        public void process(String url, Context context, Iterable<Long> iterable, Collector<UrlViewCount> collector) throws Exception {
            Long count = iterable.iterator().next();
            Long startWindowTime = context.window().getStart();
            Long endWindowTime = context.window().getEnd();
            collector.collect(new UrlViewCount(url,count,startWindowTime,endWindowTime));
        }
    }
}
