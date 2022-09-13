import com.base.wordcount.WordCount;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {

    public static Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) throws Exception {
        log.info("Executing the main method.");
        WordCount wordCount = new WordCount(args);
        wordCount.execute();
        log.info("Execution of main method is completed.");
    }
}
