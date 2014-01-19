package com.gdiama.utils.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.io.IOUtils;

import java.io.*;


public class S3BucketWrapper {

    private static final String BUCKET_NAME = "gd-test-1";
    private AmazonS3 s3;

    public void doIt() throws IOException {
        String key = "MyObjectKey";

        s3 = createClient();
        listBuckets();
//        createObject(key);
        listObjectsInBucket(BUCKET_NAME);
//        displayContentOf(BUCKET_NAME, key);
    }

    private void displayContentOf(String bucketName, String key, long size) throws IOException {
        S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
        ObjectMetadata objectMetadata = object.getObjectMetadata();

        System.out.println("Content-Type: " + objectMetadata.getContentType());
        displayTextInputStream(object.getObjectContent(), key, size);
    }

    private void listObjectsInBucket(final String bucketName) throws IOException {
        System.out.println("Listing objects");
        ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                .withBucketName(bucketName));
//                .withPrefix("My"));
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            String key = objectSummary.getKey();
            long size = objectSummary.getSize();
            System.out.println(" - " + key + "  " +
                    "(size = " + size + ")");
            displayContentOf(bucketName, key, size);
        }
        System.out.println();
    }

    private void createObject(String key) throws IOException {
        PutObjectRequest putObjectRequest = new PutObjectRequest(BUCKET_NAME, key, createSampleFile());
        s3.putObject(putObjectRequest);
    }

    private AmazonS3 createClient() {
        AmazonS3Client s3 = new AmazonS3Client(new AWSCredentials() {

            public String getAWSAccessKeyId() {
                return "AKIAJY2YGNPEDLO5HFTQ";
            }

            public String getAWSSecretKey() {
                return "o0ICyoyni9bFsJqv6JSooivMD+YNKZ/vzFNL71zz";
            }
        });

        s3.setRegion(Region.getRegion(Regions.EU_WEST_1));
        return s3;
    }

    private void listBuckets() {
        for (Bucket bucket : s3.listBuckets()) {
            System.out.println(" - " + bucket.getName());
        }
        System.out.println();
    }

    private static void displayTextInputStream(InputStream input, String key, long size) throws IOException {
        File file = new File("/Users/georgediamantidis/Desktop/test-index", key);

        byte[] buffer = new byte[(int) size];
        IOUtils.readFully(input, buffer);
        new FileOutputStream(file).write(buffer);

//        InputStreamReader in = new InputStreamReader(input);
//
//        BufferedReader reader = new BufferedReader(in);


//        while (true) {
//            String line = reader.readLine();
//            if (line == null) break;
//
//
//            System.out.println("    " + line);
//        }
//        System.out.println();
    }

    private static File createSampleFile() throws IOException {
        File file = File.createTempFile("aws-java-sdk-", ".txt");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.write("01234567890112345678901234\n");
        writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
        writer.write("01234567890112345678901234\n");
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.close();

        return file;
    }

    public static void main(String[] args) throws IOException {
        new S3BucketWrapper().doIt();
    }
}
