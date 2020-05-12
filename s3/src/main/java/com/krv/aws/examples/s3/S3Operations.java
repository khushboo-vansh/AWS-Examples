package com.krv.aws.examples.s3;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.EmailAddressGrantee;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * @author Khushboo Raj Vansh
 *
 */
public class S3Operations {

  /**
   * @param s3
   * @param bucket_name
   * @param region
   */
  public static void DeleteMyBucket(String bucket_name, String region) {
    try {
      AmazonS3 s3 = AmazonS3ClientBuilder.standard()
          .withCredentials(new ProfileCredentialsProvider()).withRegion(region).build();
      System.out.format("\nDeleting the bucket named '%s'...\n\n", bucket_name);
      s3.deleteBucket(bucket_name);
    } catch (AmazonS3Exception e) {
      System.err.println(e.getErrorMessage());
    }
  }

  /**
   * @param s3
   * @param bucket_name
   * @param region
   */
  public static void CreateNewBuckets(AmazonS3 s3, String bucket_name, String region) {
    if (s3.doesBucketExistV2(bucket_name)) {
      System.out.format("\nCannot create the bucket. \n" + "A bucket named '%s' already exists.",
          bucket_name);
    } else {
      try {
        System.out.format("\nCreating a new bucket named '%s'...\n\n", bucket_name);
        s3.createBucket(new CreateBucketRequest(bucket_name, region));
      } catch (AmazonS3Exception e) {
        System.err.println(e.getErrorMessage());
      }
    }
  }

  /**
   * @param s3
   */
  public static void ListMyBuckets(AmazonS3 s3) {
    List<Bucket> buckets = s3.listBuckets();
    System.out.println("My buckets now are:");
    for (Bucket b : buckets) {
      System.out.println(b.getName());
    }
  }

  public static void GetBucketProperties(AmazonS3 s3, String bucketName) {
    System.out.println("Bucket Region : " + s3.getRegionName());
    System.out.println("Bucket Location : " + s3.getBucketLocation(bucketName));
    System.out.println("Bucket ACL : " + s3.getBucketAcl(bucketName));
    System.out.println(
        "Bucket Cross Origin Config : " + s3.getBucketCrossOriginConfiguration(bucketName));
    // System.out.println("Bucket Encryption : " + s3.getBucketEncryption(bucketName));
    System.out
        .println("Bucket Life Cycle Config : " + s3.getBucketLifecycleConfiguration(bucketName));
    System.out.println("Bucket Logging Config : " + s3.getBucketLoggingConfiguration(bucketName));
    System.out.println(
        "Bucket Notification Config : " + s3.getBucketNotificationConfiguration(bucketName));
    System.out.println("Bucket Policy : " + s3.getBucketPolicy(bucketName));
    System.out.println("Bucket Tagging Config : " + s3.getBucketTaggingConfiguration(bucketName));
    System.out.println("Bucket URL  : " + s3.getUrl(bucketName, "ABC"));
  }

  public static void getBucketAcl(String bucket_name, Regions region) {
    System.out.println("Retrieving ACL for bucket: " + bucket_name);
    AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
    try {
      AccessControlList acl = s3.getBucketAcl(bucket_name);
      List<Grant> grants = acl.getGrantsAsList();
      for (Grant grant : grants) {
        System.out.format("  %s: %s\n", grant.getGrantee().getIdentifier(),
            grant.getPermission().toString());
      }
    } catch (AmazonServiceException e) {
      System.err.println(e.getErrorMessage());
      System.exit(1);
    }
  }

  public static void getObjects(String bucket_name, Regions region) {
    System.out.format("Objects in S3 bucket %s:\n", bucket_name);
    final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
    ListObjectsV2Result result = s3.listObjectsV2(bucket_name);
    List<S3ObjectSummary> objects = result.getObjectSummaries();
    for (S3ObjectSummary os : objects) {
      System.out.println("* " + os.getKey());
    }
  }

  public static void putObjects(String bucket_name, Regions region, String path) {
    String key_name = Paths.get(path).getFileName().toString();
    System.out.format("Uploading %s to S3 bucket %s...\n", path, bucket_name);
    final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
    try {
      s3.putObject(bucket_name, key_name, new File(path));
    } catch (AmazonServiceException e) {
      System.err.println(e.getErrorMessage());
      System.exit(1);
    }
    System.out.println("Done!");
  }

  public static void deleteObject(String bucketName, Regions region, String objectKey) {
    System.out.format("Deleting object %s from S3 bucket: %s\n", objectKey, bucketName);
    final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
    try {
      s3.deleteObject(bucketName, objectKey);
    } catch (AmazonServiceException e) {
      System.err.println(e.getErrorMessage());
      System.exit(1);
    }
    System.out.println("Done!");
  }

  public static void deleteObjects(String bucketName, Regions region, String[] objectKey) {
    System.out.println("Deleting objects from S3 bucket: " + bucketName);
    for (String k : objectKey) {
      System.out.println(" * " + k);
    }
    AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
    try {
      DeleteObjectsRequest dor = new DeleteObjectsRequest(bucketName).withKeys(objectKey);
      s3.deleteObjects(dor);
    } catch (AmazonServiceException e) {
      System.err.println(e.getErrorMessage());
      System.exit(1);
    }
    System.out.println("Done!");
  }

  public static void CreateBucketWithAcl() {
    // Regions clientRegion = Regions.DEFAULT_REGION;
    Regions clientRegion = Regions.US_EAST_2;
    String bucketName = "khushboo1234";
    String userEmailForReadPermission = "khushi.raj.vansh@gmail.com";

    try {
      AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(clientRegion).build();

      // Create a bucket with a canned ACL. This ACL will be replaced by the setBucketAcl()
      // calls below. It is included here for demonstration purposes.
      CreateBucketRequest createBucketRequest =
          new CreateBucketRequest(bucketName, clientRegion.getName());
      // .withCannedAcl(CannedAccessControlList.LogDeliveryWrite);
      s3Client.createBucket(createBucketRequest);

      // Create a collection of grants to add to the bucket.
      ArrayList<Grant> grantCollection = new ArrayList<Grant>();

      // Grant the account owner full control.
      Grant grant1 = new Grant(new CanonicalGrantee(s3Client.getS3AccountOwner().getId()),
          Permission.FullControl);
      grantCollection.add(grant1);

      // Grant the LogDelivery group permission to write to the bucket.
      Grant grant2 = new Grant(GroupGrantee.LogDelivery, Permission.Write);
      grantCollection.add(grant2);

      // Save grants by replacing all current ACL grants with the two we just created.
      AccessControlList bucketAcl = new AccessControlList();
      bucketAcl.grantAllPermissions(grantCollection.toArray(new Grant[0]));
      s3Client.setBucketAcl(bucketName, bucketAcl);

      // Retrieve the bucket's ACL, add another grant, and then save the new ACL.
      AccessControlList newBucketAcl = s3Client.getBucketAcl(bucketName);
      Grant grant3 =
          new Grant(new EmailAddressGrantee(userEmailForReadPermission), Permission.Read);
      newBucketAcl.grantAllPermissions(grant3);
      s3Client.setBucketAcl(bucketName, newBucketAcl);
    } catch (AmazonServiceException e) {
      // The call was transmitted successfully, but Amazon S3 couldn't process
      // it and returned an error response.
      e.printStackTrace();
    } catch (SdkClientException e) {
      // Amazon S3 couldn't be contacted for a response, or the client
      // couldn't parse the response from Amazon S3.
      e.printStackTrace();
    }
  }

}
