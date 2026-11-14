package com.marktstorm.functions;

import com.marktstorm.model.ProductAccumulator;
import com.marktstorm.model.ProductAggregationResult;
import com.marktstorm.model.Order;

import org.apache.flink.api.common.functions.AggregateFunction;

public class ProductAggregator implements AggregateFunction<Order, ProductAccumulator, ProductAggregationResult>{
    @Override 
    public ProductAccumulator createAccumulator(){
        return new ProductAccumulator();
    }

    @Override 
    public ProductAccumulator add (Order order, ProductAccumulator accumulator){
        accumulator.orderCount++;
        accumulator.totalQuantity += order.quantity;
        accumulator.totalRevenue += (order.price * order.quantity);
        if(accumulator.product == null){
            accumulator.product = order.product;
        }
        if(accumulator.category == null){
            accumulator.category = order.category;
        }
        return accumulator;
    }

    @Override 
    public ProductAggregationResult getResult(ProductAccumulator accumulator){
        ProductAggregationResult result = new ProductAggregationResult();
        result.orderCount = accumulator.orderCount;
        result.totalQuantity = accumulator.totalQuantity;
        result.totalRevenue = accumulator.totalRevenue;
        result.product = accumulator.product;
        result.category = accumulator.category;

        return result;

    }

    @Override 
    public ProductAccumulator merge(ProductAccumulator a, ProductAccumulator b){
        a.orderCount += b.orderCount;
        a.totalQuantity += b.totalQuantity;
        a.totalRevenue += b.totalRevenue;
        if(a.product == null){
            a.product = b.product;
        }
        if(a.category == null){
            a.category = b.category;
        }
        return a;
    }
}
