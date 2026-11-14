package com.marktstorm.model;
import java.io.Serializable;

public class OrderAggregationResult implements Serializable {
    
    public String groupType;
    public String groupKey;
    public long orderCount;
    public long totalQuantity;
    public double totalRevenue;
    public long windowStart;
    public long windowEnd;

    public OrderAggregationResult(){
        
    }

}
