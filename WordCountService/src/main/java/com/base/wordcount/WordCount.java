package com.base.wordcount;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCount {

    public static Logger log = LoggerFactory.getLogger(WordCount.class);
    public ExecutionEnvironment environment;
    public ParameterTool parameters;

    public WordCount(String[] args) {
        log.info("Initializing Word Count.");
        environment = ExecutionEnvironment.getExecutionEnvironment();
        parameters = ParameterTool.fromArgs(args);
        environment.getConfig().setGlobalJobParameters(parameters);
        log.info("Constructor Execution Completed.");
    }

    public void execute() throws Exception {
        log.info("Inside execute().");
        DataSet<String> words = environment.readTextFile(parameters.get("input"));
        log.info("Text file read and stored for further processing.");
        DataSet<String> filteredWords = words.filter((FilterFunction<String>) text -> text.startsWith("N"));
        DataSet<Tuple2<String, Integer>> tokenized = filteredWords.map(new Tokenizer());
        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(new int[]{0})
                .sum(1);
        if(parameters.has("filter")) {
            filteredWords.writeAsText(parameters.get("filter"));
        }
        if(parameters.has("token")) {
            tokenized.writeAsCsv(parameters.get("token"), "\n", " ");
        }
        if(parameters.has("output")) {
            counts.writeAsCsv(parameters.get("output"), "\n", " ");
        }
        environment.execute("Words Count");
    }
}
