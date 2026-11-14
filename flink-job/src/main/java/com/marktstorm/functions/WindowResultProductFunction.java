package com.marktstorm.functions;
import com.marktstorm.model.ProductAggregationResult;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class WindowResultProductFunction extends ProcessWindowFunction<ProductAggregationResult, ProductAggregationResult, String, TimeWindow> {
    
    private final String groupType;

    public WindowResultProductFunction(String groupType){
        this.groupType = groupType;
    }

    @Override 
    public void process(String key, Context ctx, Iterable<ProductAggregationResult> elements, Collector<ProductAggregationResult> out){
        ProductAggregationResult result = elements.iterator().next();
        result.groupType = groupType;
        result.groupKey = key;
        result.windowStart = ctx.window().getStart();
        result.windowEnd = ctx.window().getEnd();
        out.collect(result);
    } 

}
