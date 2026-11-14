package com.marktstorm.functions;
import com.marktstorm.model.OrderAccumulator;
import com.marktstorm.model.OrderAggregationResult;
import com.marktstorm.model.Order;

import org.apache.flink.api.common.functions.AggregateFunction;


public class OrderAggregator implements AggregateFunction<Order, OrderAccumulator, OrderAggregationResult>{

    @Override 
    public OrderAccumulator createAccumulator(){
        return new OrderAccumulator();
    }

    @Override 
    public OrderAccumulator add(Order order, OrderAccumulator accumulator){
        accumulator.orderCount++;
        accumulator.totalQuantity += order.quantity;
        accumulator.totalRevenue += (order.price * order.quantity);
        return accumulator;
    }

    @Override 
    public OrderAggregationResult getResult(OrderAccumulator accumulator){
        OrderAggregationResult result = new OrderAggregationResult();
        result.orderCount = accumulator.orderCount;
        result.totalQuantity = accumulator.totalQuantity;
        result.totalRevenue = accumulator.totalRevenue;
        return result;
    }

    @Override 
    public OrderAccumulator merge(OrderAccumulator a, OrderAccumulator b){
        a.orderCount += b.orderCount;
        a.totalQuantity += b.totalQuantity;
        a.totalRevenue += b.totalRevenue;
        return a;
    }


}

