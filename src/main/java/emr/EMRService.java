package emr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;

public class EMRService {

    private S3Manager s3Manager;
    private static Set<String> inputFiles=new HashSet<>();
    private EMRPublisher emrPublisher;
    private static final Logger logger = LoggerFactory.getLogger(EMRService.class);

    public EMRService(){
        this.s3Manager=new S3Manager();
        this.emrPublisher=new EMRPublisher();
    }

    public void trigerActions()  {
        logger.info("EMR Service Thread Initiated");
        int count=1;
        while (true && count<=Constants.JOB_COUNT){
            Set<String> filesAvailableInS3= new HashSet<>(s3Manager.getObjectsInsideTheBucket(Constants.INPUT_BUCKET)) ;
            if(filesAvailableInS3.size()>inputFiles.size()){
                count++;
                filesAvailableInS3.removeAll(inputFiles);
                inputFiles.addAll(filesAvailableInS3);
                filesAvailableInS3.forEach((x)-> {
                    logger.info("New File Found:{}",x);
                    List<String> resultJobIs= emrPublisher.publishAction(
                            Constants.PRIMARY_JAR,
                            Constants.INPUT_FILE_PATH.concat(x),
                            Constants.OUTPUT_FILE_PATH_PREFIX.concat(x),
                            Constants.PRIMARY_JOB_ID_PREFIX.concat(x),
                            Constants.CLUSTER_ID
                    );

                    resultJobIs.forEach(jobId->{
                        Executors.newSingleThreadExecutor().execute(()->new CombinerService().combine(Constants.CLUSTER_ID,jobId,Constants.OUTPUT_FOLDER_PREFIX.concat(x),true));
                    });
                });
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                logger.error("Exception Occurred in triggering new Job",e);
            }
        }
        logger.info("EMR Service Thread Terminated");
    }

    public void triggerFinalActions(){
        logger.info("Triggered Final actions");
        s3Manager.putTextFile(Constants.INPUT_BUCKET,Constants.CONSOLIDATED_INPUT,new DataAccumultor().getFiles());

        List<String> resultJobIs= emrPublisher.publishAction(
                Constants.SECONDARY_JAR,
                Constants.SECONDARY_INPUT_LOCATION,
                Constants.SECONDARY_OUTPUT_LOCATION,
                Constants.SECONDARY_JOB_ID,
                Constants.CLUSTER_ID
        );
        s3Manager.putTextFile(Constants.OUTPUT_BUCKET,Constants.END_RESULT,new CombinerService().combine(Constants.CLUSTER_ID,resultJobIs.get(0),Constants.OUTPUT_FINAL,false));
        logger.info("********************EOP********************");
        System.exit(0);
    }


}
