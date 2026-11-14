package com.marktstorm;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.connector.base.DeliveryGuarantee;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.marktstorm.model.Order;
import com.marktstorm.model.DeadLetter;
import com.marktstorm.processor.OrderValidation;
import com.marktstorm.functions.OrderAggregator;
import com.marktstorm.functions.ProductAggregator;
import com.marktstorm.functions.WindowResultOrdersFunction;
import com.marktstorm.functions.WindowResultProductFunction;
import com.marktstorm.model.OrderAggregationResult;
import com.marktstorm.model.OrderAccumulator;
import com.marktstorm.model.ProductAccumulator;
import com.marktstorm.model.ProductAggregationResult;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.time.Duration;

public class MarktstormJob{
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("orders")
            .setGroupId("marktstorm-normal-production-group")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
        
        DataStream<String> orderStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Normal Production Source");
        
        
        SingleOutputStreamOperator<Order> validOrders = orderStream.process(new OrderValidation());
        DataStream<DeadLetter> invalidOrders = validOrders.getSideOutput(OrderValidation.deadLetterOutputTag);

        DataStream<Order> timeStampedOrders = validOrders.assignTimestampsAndWatermarks(
            WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((order,timestamp) -> order.event_timestamp)
        );

        DataStream<OrderAggregationResult> orderResult = timeStampedOrders
            .keyBy(order -> order.order_id)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .aggregate(new OrderAggregator(), new WindowResultOrdersFunction("orders"));
        
        DataStream<ProductAggregationResult> productResult = timeStampedOrders
            .keyBy(order -> order.product_id)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .aggregate(new ProductAggregator(), new WindowResultProductFunction("product"));

        

        


    }
}