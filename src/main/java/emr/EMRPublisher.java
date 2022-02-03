package emr;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class EMRPublisher {

    private AmazonElasticMapReduce emr;
    private static final Logger logger = LoggerFactory.getLogger(EMRPublisher.class);

    public EMRPublisher() {
        AWSCredentials credentials_profile = null;
        try {
            credentials_profile = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load credentials from .aws/credentials file. " +
                            "Make sure that the credentials file exists and the profile name is specified within it.",
                    e);
        }

        this.emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials_profile))
                .withRegion(Regions.AP_SOUTH_1)
                .build();
    }

    public List<String> publishAction(String jarLocation, String inputLocation, String outputLocation, String jobName, String clusterId) {

        HadoopJarStepConfig hadoopConfig = new HadoopJarStepConfig()
                .withJar(jarLocation)
                .withArgs(inputLocation, outputLocation);

        StepConfig myCustomJarStep = new StepConfig(jobName, hadoopConfig);
        myCustomJarStep.setActionOnFailure("CONTINUE");

        AddJobFlowStepsResult result = emr.addJobFlowSteps(new AddJobFlowStepsRequest()
                .withJobFlowId(clusterId)
                .withSteps(myCustomJarStep));

        logger.info("JobIds:{}",result.getStepIds());
        return result.getStepIds();
    }

    public String checkStatus(String clusterId, String stepId){
        DescribeStepResult describeStepResult= emr.describeStep(new DescribeStepRequest().withClusterId(clusterId).withStepId(stepId));
        return describeStepResult.getStep().getStatus().getState();
    }

}
