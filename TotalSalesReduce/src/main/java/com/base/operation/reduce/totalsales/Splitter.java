package com.base.operation.reduce.totalsales;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;

public class Splitter implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>> {
    @Override
    public Tuple5<String, String, String, Integer, Integer> map(String value) {
        String[] words = value.split(",");
        return new Tuple5<>(words[1], words[2], words[3], Integer.parseInt(words[4]), 1);
    }
}
