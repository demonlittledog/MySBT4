import java.util.logging.Logger;

public class LoggerGenerator {
    private static Logger logger = Logger.getLogger(LoggerGenerator.class.getName());
    public static void main(String[] args) throws Exception{
        int index = 0;
        while (true){
            Thread.sleep(1000);
            logger.info("current value is :"+index++);
        }
    }
}
