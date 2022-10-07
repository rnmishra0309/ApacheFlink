package com.stream.service;

import com.stream.constants.Constants;
import com.stream.utils.FilterNonTrips;
import com.stream.utils.RawDataSplitter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;

public class CabStreamService {
    public static Logger log = LogManager.getLogger(CabStreamService.class);

    public StreamExecutionEnvironment environment;
    public ParameterTool parameters;
    public InputStream inputStream;

    public CabStreamService() throws Exception {
        log.info("Setting up environment.");
        environment = StreamExecutionEnvironment.getExecutionEnvironment();
        inputStream = CabStreamService.class.getClassLoader().getResourceAsStream("config.properties");
        parameters = ParameterTool.fromPropertiesFile(inputStream);
        environment.getConfig().setGlobalJobParameters(parameters);
        log.info("Global parameters are set. And ready for execution");
    }

    public void execute() throws Exception {
        log.info("Starting the execution.");
        log.info("Getting the raw stream data");
        DataStream<String> cabRawData = environment.readTextFile(parameters.get(Constants.CAB_RAW_DATA_PATH));

        DataStream<Tuple8<String,String,String,String,String,String,String,Integer>>
                splittedData = cabRawData.flatMap(new RawDataSplitter());

        DataStream<Tuple8<String,String,String,String,String,String,String,Integer>>
                filterData = splittedData.filter(new FilterNonTrips());

        DataStream<Tuple8<String, String, String, String, String, String, String, Integer>>
                groupedOnDestination = filterData.keyBy(6).sum(7);

//        DataStream<Tuple8<String, String, String, String, String, String, String, Integer>>
//                totalOnDestination = groupedOnDestination.keyBy(6).max(7);

        log.info("Saving the results.");

        splittedData.writeAsCsv(parameters.get(Constants.CAB_AVERAGE_PASSENGER_OUTPUT_PATH),
                FileSystem.WriteMode.OVERWRITE, "\n", " ");

        groupedOnDestination.writeAsCsv(parameters.get(Constants.CAB_POPULAR_DESTINATION_OUTPUT_PATH),
                FileSystem.WriteMode.OVERWRITE, "\n", " ");

        environment.execute("cab data stream");
    }

}
