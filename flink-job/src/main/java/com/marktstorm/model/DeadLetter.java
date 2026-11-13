package com.marktstorm.model;
import java.io.Serializable;


public class DeadLetter implements Serializable{
    public String message;
    public String error;
    public long rejectedAt;

    public DeadLetter(){

    }
    public DeadLetter(String message, String error, long rejectedAt){
        this.message = message;
        this.error = error;
        this.rejectedAt = rejectedAt;
    }
}
