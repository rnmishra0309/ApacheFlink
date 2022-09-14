import com.base.operation.reduce.totalsales.TotalSalesReduce;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {
    public static Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) throws Exception {
        log.info("Executing the main method.");
        TotalSalesReduce totalSalesReduce = new TotalSalesReduce();
        totalSalesReduce.execute();
        log.info("Execution of main method is completed.");
    }
}
