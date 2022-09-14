import com.base.stream.wordcount.WordCountStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {
    public static Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) throws Exception {
        log.info("Executing the main method.");
        WordCountStream wordCountStream = new WordCountStream(args);
        wordCountStream.execute();
        log.info("Execution of main method is completed.");
    }
}
