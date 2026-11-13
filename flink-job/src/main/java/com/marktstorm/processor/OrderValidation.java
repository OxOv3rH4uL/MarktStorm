package com.marktstorm.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.marktstorm.model.Order;
import com.marktstorm.model.DeadLetter;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


public class OrderValidation extends ProcessFunction<String, Order>{
    //for invalid events, pushing it to dead letter topic
    public static final OutputTag<DeadLetter> deadLetterOutputTag = new OutputTag<DeadLetter>("dead-letter") {};
    // transient doesnt serialize the object/this mapper
    private transient ObjectMapper mapper;

    @Override 
    public void open(org.apache.flink.configuration.Configuration parameters){
        mapper = new ObjectMapper();
    }

    @Override 
    public void processElement(String message, Context ctx, Collector<Order> out) throws Exception{
        try{
            Order order = mapper.readValue(message, Order.class);
            if (order.order_id == null ||
                order.user_id == null ||
                order.product_id == null ||
                order.product == null ||
                order.category == null 
            ){
                throw new IllegalArgumentException("Invalid Orders");
            }if(order.order_id.isBlank() ||
                order.user_id.isBlank() ||
                order.product_id.isBlank() ||
                order.product.isBlank() ||
                order.category.isBlank()
            ){
                throw new IllegalArgumentException("Missing fields");
            }if(order.quantity <= 0){
                throw new IllegalArgumentException("Invalid Quantity");
            }if(order.price <= 0 || !Double.isFinite(order.price)){
                throw new IllegalArgumentException("Invalid price");
            }if(order.event_timestamp <= 0){
                throw new IllegalArgumentException("Invalid timestamp");
            }
            out.collect(order);

        }catch(Exception e){
            ctx.output(deadLetterOutputTag, new DeadLetter(message, e.getMessage(), System.currentTimeMillis()));
        }
    }
}