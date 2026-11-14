package com.marktstorm.model;
import java.io.Serializable;
public class ProductAggregationResult {
    public String groupType;
    public String groupKey;
    public long orderCount;
    public long totalQuantity;
    public double totalRevenue;
    public String product;
    public String category;
    public long windowStart;
    public long windowEnd;

    public ProductAggregationResult(){
        
    }
}

