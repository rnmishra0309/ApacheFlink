package com.base.stream.wordcount;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class WordCountStream {

    public static Logger log = LoggerFactory.getLogger(WordCountStream.class);

    public StreamExecutionEnvironment environment;
    public ParameterTool parameters;

    public WordCountStream(String[] args) {
        environment = StreamExecutionEnvironment.getExecutionEnvironment();
        parameters = ParameterTool.fromArgs(args);
        environment.getConfig().setGlobalJobParameters(parameters);
    }

    public void execute() throws Exception {
        InputStream input = WordCountStream.class.getClassLoader().getResourceAsStream("config.properties");
        Properties properties = new Properties();
        if(input == null) {
            log.info("Properties file not found.");
            return;
        }
        properties.load(input);

        DataStream<String> textData = environment.socketTextStream(properties.getProperty("flink.socket.host"),
                Integer.parseInt(properties.getProperty("flink.socket.port")));

        DataStream<Tuple2<String, Integer>> finalOutput = textData.filter(
                (FilterFunction<String>) value -> value.startsWith("N")
                )
                .map(new Tokenizer())
                .keyBy(0).sum(1);

        finalOutput.print();

        environment.execute("Word Count Stream");
    }

}
