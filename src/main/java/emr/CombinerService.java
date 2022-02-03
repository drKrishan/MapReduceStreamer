package emr;

import com.amazonaws.services.s3.model.S3Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

public class CombinerService {

    private static final Logger logger = LoggerFactory.getLogger(CombinerService.class);
    private EMRPublisher emrPublisher;
    private S3Manager s3Manager;

    public CombinerService(){
        this.s3Manager=new S3Manager();
        this.emrPublisher=new EMRPublisher();
    }

    public String combine(String clusterId, String jobId, String outputFolder,Boolean accumulate) {
        logger.info("Combine Service Thread Initiated for JobID:{}, JobType:{}",jobId, (accumulate==true)?"PRIMARY":"SECONDARY");

        String status=emrPublisher.checkStatus(clusterId,jobId);
        while (!status.equalsIgnoreCase("COMPLETED")){
            try {
                status=emrPublisher.checkStatus(clusterId,jobId);
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        logger.info("Start processing for folder:{}, for JobID:{}",outputFolder,jobId);
        List<String> files= s3Manager.getObjectsListFromFolder(Constants.OUTPUT_BUCKET,outputFolder).stream().filter(x->!x.contains("SUCCESS")).collect(Collectors.toList());
        logger.info("files available in output folder:{}, files:{}  ",outputFolder,files);
        StringBuilder stringBuilder=new StringBuilder();
        files.forEach(file->{
            S3Object s3Object= s3Manager.getObject(Constants.OUTPUT_BUCKET,file);
            InputStream objectData = s3Object.getObjectContent();

            try{
                BufferedReader reader= new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));

                String line;
                while ((line= reader.readLine())!=null){
                    stringBuilder.append(line+"\n");
                }
                objectData.close();
            }catch (IOException ex){
                ex.printStackTrace();
            }
        });
        DataAccumultor dataAccumultor=new DataAccumultor();
        if(accumulate==true) dataAccumultor.addFile(stringBuilder.toString());
        logger.info("Combine Service Thread Terminated for JobID:{}, JobType:{}",jobId, (accumulate==true)?"PRIMARY":"SECONDARY");
        return stringBuilder.toString();
    }

}
