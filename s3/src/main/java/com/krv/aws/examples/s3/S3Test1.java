package com.krv.aws.examples.s3;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class S3Test1 {

  private static AmazonS3 s3;

  public static void main(String[] args) {
    if (args.length < 2) {
      System.out.format("Usage: <the bucket name> <the AWS Region to use>\n"
          + "Example: my-test-bucket us-east-2\n");
      return;
    }
    // khushboo101 us-east-2
    String bucket_name = args[0];
    String region = args[1];

    s3 = AmazonS3ClientBuilder.standard().withCredentials(new ProfileCredentialsProvider())
        .withRegion(region).build();

    S3Operations.getBucketAcl(bucket_name, Regions.US_EAST_2);
    String[] keys = {"AWS.JPG", "test/signature.jpg"};
    // S3Operations.putObjects(bucket_name, Regions.US_EAST_2, "C:\\KRV\\GitWorkFlow.JPG");
    // S3Operations.deleteObject(bucket_name, Regions.US_EAST_2, "GitWorkFlow.JPG");
    S3Operations.deleteObjects(bucket_name, Regions.US_EAST_2, keys);
    S3Operations.getObjects(bucket_name, Regions.US_EAST_2);
    // S3Operations.GetBucketProperties(s3, bucket_name);

    // List current buckets.
    S3Operations.ListMyBuckets(s3);

    // Create a new bucket
    // S3Operations.CreateNewBuckets(s3, bucket_name + "new", region);

    // Confirm that the bucket was created.
    // S3Operations.ListMyBuckets(s3);

    // Delete the Newly created Bucket
    // S3Operations.DeleteMyBucket(bucket_name + "new", "us-east-2");
    // S3Operations.DeleteMyBucket("khushboo1234", "us-west-2");

    // Confirm that the bucket was deleted.
    // S3Operations.ListMyBuckets(s3);
  }
}


