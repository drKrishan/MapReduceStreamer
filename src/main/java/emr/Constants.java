package emr;

public class Constants {
    public static final Integer JOB_COUNT = 3;
    public static final String INPUT_BUCKET = "input-files-emr";
    public static final String OUTPUT_BUCKET = "emr-output-files";
    public static final String INPUT_FILE_PREFIX = "input_";
    public static final String OUTPUT_FOLDER_PREFIX = "outputOf_";
    public static final String PRIMARY_JAR="s3://emr-jar-msc/logEMR_WordCount.jar";
    public static final String INPUT_FILE_PATH="s3://input-files-emr/";
    public static final String OUTPUT_FILE_PATH_PREFIX = "s3://emr-output-files/outputOf_";
    public static final String PRIMARY_JOB_ID_PREFIX = "PrimaryJobFor_";
    public static final String CLUSTER_ID = "j-2O1H0UVOLFDID";
    public static final String CONSOLIDATED_INPUT = "consolidatedInput";

    public static final String SECONDARY_JAR = "s3://emr-jar-msc/finalCounter.jar";
    public static final String SECONDARY_INPUT_LOCATION = "s3://input-files-emr/consolidatedInput";
    public static final String SECONDARY_OUTPUT_LOCATION = "s3://emr-output-files/outputFinal";
    public static final String SECONDARY_JOB_ID = "JobForFinal";
    public static final String OUTPUT_FINAL="outputFinal";
    public static final String END_RESULT="endResult";

}
