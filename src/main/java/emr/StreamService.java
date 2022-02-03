package emr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class StreamService {

    private static final Logger logger = LoggerFactory.getLogger(StreamService.class);
    List<String> words=new ArrayList<>();
    List<String> filteredWords=new ArrayList<>();
    static final int max=1000;
    static final int min=0;
    static final int buffer_length=2000;
    S3Manager s3Manager;

    public StreamService(){
        try{
            words = Files.readAllLines(Paths.get("wordlist.txt"));
            IntStream.range(0, 1001).forEach(index -> filteredWords.add(words.get(index*60)));
            this.s3Manager=new S3Manager();
        }catch (Exception e){
           logger.info("Exception Occurred in reading file");
        }
    }

    public void startConsumption(){
        logger.info("Stream Service Initiated");
        StringBuilder sb=new StringBuilder();
        int count=1;
        int fileCount=1;

        while(true && fileCount<=Constants.JOB_COUNT){
            try {
                int b = (int)(Math.random()*(max-min+1)+min);
                sb.append(filteredWords.get(b));
                sb.append((count %5==0)? ("\n"): (" "));

                if(count==buffer_length){
                    String fileName=Constants.INPUT_FILE_PREFIX.concat(String.valueOf(fileCount));
                    logger.info("pushed file:{} to S3",fileName);
                    s3Manager.putTextFile(Constants.INPUT_BUCKET,fileName, sb.toString());
                    sb.setLength(0);
                    count=1;
                    fileCount++;
                }
                Thread.sleep(2);
                count++;
            } catch (InterruptedException e) {
                logger.error("Exception occurred in Stream Processing",e);
            }

        }
        logger.info("Stream Service Terminated");
    }

}
