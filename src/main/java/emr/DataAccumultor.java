package emr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataAccumultor {
    private static final Logger logger = LoggerFactory.getLogger(DataAccumultor.class);
    private static int counter = 0;
    private static String files = "";
    private EMRService emrService;

    public DataAccumultor() {
        this.emrService = new EMRService();
    }

    public void addFile(String file) {

        files = files.concat(file);
        counter++;
        try {
            Thread.sleep(1000);
            if (counter == Constants.JOB_COUNT) {
                emrService.triggerFinalActions();
            }
        } catch (Exception ex) {
            logger.error("Exception occured while accumulating", ex);
        }

    }

    public String getFiles() {
        return files;
    }


}
