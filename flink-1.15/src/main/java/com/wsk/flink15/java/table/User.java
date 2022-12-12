package com.wsk.flink15.java.table;

import java.time.Instant;

/**
 * @description:
 * @author: wsk
 * @date: 2021/6/25 17:40
 * @version: 1.0
 */
public class User {
    public String name;

    public Integer score;

    public Instant event_time;

    // default constructor for DataStream API
    public User() {}

    // fully assigning constructor for Table API
    public User(String name, Integer score, Instant event_time) {
        this.name = name;
        this.score = score;
        this.event_time = event_time;
    }
}
