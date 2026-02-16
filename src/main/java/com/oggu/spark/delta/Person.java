package com.oggu.spark.delta;
/**
 *
 * Author : bhask
 * Created : 02-15-2026
 */ // Simple Java Bean (POJO)
public class Person implements java.io.Serializable {

    private int id;
    private String name;

    // Required: no-arg constructor
    public Person() {
    }

    public Person(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
