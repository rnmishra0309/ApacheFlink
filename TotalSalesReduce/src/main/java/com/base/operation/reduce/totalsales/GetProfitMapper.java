package com.base.operation.reduce.totalsales;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple2;

public class GetProfitMapper implements
        MapFunction<Tuple5< String, String, String, Integer, Integer >, Tuple2 < String, Double >> {
    @Override
    public Tuple2<String, Double> map(Tuple5<String, String, String, Integer, Integer> value) {
        return new Tuple2<>(
                value.f0,
                value.f3*1.0/value.f4
        );
    }
}
