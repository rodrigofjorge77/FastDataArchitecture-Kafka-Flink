package Dto;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class Transaction {
    private int id;
    private String uid;
    private String color;
    private String department;
    private String material;
    private String product_name;
    private double price;
    private String price_string;
    private String promo_code;
}
