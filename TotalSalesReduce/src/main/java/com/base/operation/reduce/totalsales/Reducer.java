package com.base.operation.reduce.totalsales;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;

public class Reducer implements ReduceFunction<Tuple5<String, String, String, Integer, Integer>> {
    @Override
    public Tuple5<String, String, String, Integer, Integer> reduce(
            Tuple5<String, String, String, Integer, Integer> currentValue,
            Tuple5<String, String, String, Integer, Integer> previousValue) {

        return new Tuple5<>(
                currentValue.f0,
                currentValue.f1,
                currentValue.f2,
                currentValue.f3 + previousValue.f3,
                currentValue.f4 + previousValue.f4
        );

    }
}
