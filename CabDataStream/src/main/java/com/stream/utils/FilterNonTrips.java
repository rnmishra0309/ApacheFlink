package com.stream.utils;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple8;

public class FilterNonTrips implements FilterFunction<Tuple8<String,String,String,String,String,String,String,Integer>>{

    @Override
    public boolean filter(Tuple8<String, String, String, String, String, String, String, Integer> value) {
        return value.f4.equalsIgnoreCase("yes");
    }

}
