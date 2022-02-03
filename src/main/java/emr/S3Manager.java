package emr;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class S3Manager {

    private static final Logger logger = LoggerFactory.getLogger(S3Manager.class);
    private AmazonS3 s3;
    S3Manager(){
        this.s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.AP_SOUTH_1).build();
    }

    List<Bucket> getS3Buckets(){
        return s3.listBuckets();
    }

    List<String> getObjectsInsideTheBucket(String bucket_name){
        ListObjectsV2Result result = s3.listObjectsV2(bucket_name);
        logger.info("triggered object list inside a Bucket:{}",bucket_name);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        return objects.stream().map(x->x.getKey()).collect(Collectors.toList());
    }

    void putTextFile(String bucketName,String stringObjKeyName,String content){
        try {
            logger.info("Uploading file:{} to bucket:{}",stringObjKeyName,bucketName);
            s3.putObject(bucketName, stringObjKeyName, content);
        }catch (Exception ex){
            logger.error("Exception occurred in uploading file to S3",ex);
        }
    }

    public List<String> getObjectsListFromFolder(String bucketName, String folderKey) {

        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(bucketName)
                .withPrefix(folderKey + "/");

        List<String> keys = new ArrayList<>();

        ObjectListing objects = s3.listObjects(listObjectsRequest);

        for (; ; ) {
            List<S3ObjectSummary> summaries = objects.getObjectSummaries();
            if (summaries.size() < 1) {
                break;
            }

            summaries.forEach(s -> keys.add(s.getKey()));
            logger.info("triggered object list inside the bucket:{}, folder:{}",bucketName,folderKey);
            objects = s3.listNextBatchOfObjects(objects);
        }

        return keys;
    }

    public S3Object getObject(String bucketName, String filepath){
        logger.info("Downloading the file:{}, from:{}",filepath,bucketName);
        return s3.getObject(new GetObjectRequest(bucketName, filepath));
    }
}
