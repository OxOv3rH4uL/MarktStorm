package com.marktstorm.functions;
import com.marktstorm.model.OrderAggregationResult;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowResultOrdersFunction extends ProcessWindowFunction<OrderAggregationResult, OrderAggregationResult, String, TimeWindow> {
    
    private final String groupType;

    public WindowResultOrdersFunction(String groupType){
        this.groupType = groupType;
    }

    @Override 
    public void process(String key, Context ctx, Iterable<OrderAggregationResult> elements, Collector<OrderAggregationResult> out){
        OrderAggregationResult result = elements.iterator().next();
        result.groupType = groupType;
        result.groupKey = key;

        result.windowStart = ctx.window().getStart();
        result.windowEnd = ctx.window().getEnd();

        out.collect(result);
    }
}
