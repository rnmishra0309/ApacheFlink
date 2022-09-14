package com.base.operation.reduce.totalsales;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

public class TotalSalesReduce {

    public static Logger log = LoggerFactory.getLogger(TotalSalesReduce.class);

    public StreamExecutionEnvironment environment;
    public ParameterTool parameters;
    public InputStream inputStream;

    public TotalSalesReduce() throws Exception {
        environment = StreamExecutionEnvironment.getExecutionEnvironment();
        inputStream = TotalSalesReduce.class.getClassLoader().getResourceAsStream("config.properties");
        parameters = ParameterTool.fromPropertiesFile(inputStream);
        environment.getConfig().setGlobalJobParameters(parameters);
    }

    public void execute() throws Exception {
        log.info("Executing Job.");
        DataStream<String> text = environment.readTextFile(parameters.get("flink.input"));
        DataStream<Tuple5<String, String, String, Integer, Integer>> splitData = text.map(new Splitter());
        DataStream<Tuple5<String, String, String, Integer, Integer>> reducedData = splitData
                .keyBy(0).reduce(new Reducer());
        DataStream<Tuple2<String, Double>> profitPerMonth = reducedData.map(new GetProfitMapper());
        splitData.writeAsCsv(
                parameters.get("flink.splitter"),
                FileSystem.WriteMode.OVERWRITE,
                "\n",
                " "
        );
        reducedData.writeAsCsv(
                parameters.get("flink.reducer"),
                FileSystem.WriteMode.OVERWRITE,
                "\n",
                " "
        );
        profitPerMonth.writeAsCsv(
                parameters.get("flink.output"),
                FileSystem.WriteMode.OVERWRITE,
                "\n",
                " "
        );

        environment.execute("Total Sales Reducer");
    }
}
