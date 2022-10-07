package com.stream.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.util.Collector;

public class RawDataSplitter implements FlatMapFunction<String,
        Tuple8<String,String,String,String,String,String,String,Integer>> {

    @Override
    public void flatMap(String value,
              Collector<Tuple8<String, String, String, String, String, String, String, Integer>> collector) {
        String[] dataChunks = value.split(",");
        Tuple8<String, String, String, String, String, String, String, Integer> data = new Tuple8<>(
                dataChunks[0],
                dataChunks[1],
                dataChunks[2],
                dataChunks[3],
                dataChunks[4],
                dataChunks[5],
                dataChunks[6],
                dataChunks[7].equalsIgnoreCase("'null'") ? 0 : Integer.valueOf(dataChunks[7])
        );
        collector.collect(data);
    }
}
