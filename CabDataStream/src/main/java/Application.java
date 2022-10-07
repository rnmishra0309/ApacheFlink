import com.stream.service.CabStreamService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Application {
    public static Logger log = LogManager.getLogger(Application.class);

    public static void main(String[] args) throws Exception {
        log.info("Executing the main method.");
        CabStreamService cabStreamService = new CabStreamService();
        cabStreamService.execute();
        log.info("Execution of main method is completed.");
    }
}
