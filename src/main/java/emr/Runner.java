package emr;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class Runner {

    private static final Logger logger = LoggerFactory.getLogger(Runner.class);

    public static void main(String[] args) {
        StreamService streamService=new StreamService();
        EMRService emrService=new EMRService();
        Executor emrExecutor = Executors.newSingleThreadExecutor();
        Executor streamExecutor = Executors.newSingleThreadExecutor();
        streamExecutor.execute(streamService::startConsumption);
        emrExecutor.execute(emrService::trigerActions);
    }


}
