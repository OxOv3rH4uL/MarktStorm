
package com.marktstorm.model;
import java.io.Serializable;

public class Order implements Serializable {

    public String order_id;
    public String user_id;
    public String product_id;
    public String product;
    public String category;
    public int quantity;
    public double price;
    public long event_timestamp;
}