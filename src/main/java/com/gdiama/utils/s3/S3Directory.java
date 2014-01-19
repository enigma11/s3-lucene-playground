package com.gdiama.utils.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.apache.lucene.store.*;

import java.io.*;
import java.util.HashMap;

public class S3Directory extends NIOFSDirectory {
    private final HashMap<String, ByteArrayOutputStream> files;
    private S3ObjectInputStream inputStream;
    private AmazonS3 s3;
    private static final String KEY = "MyObjectKey";
    private static final String BUCKET_NAME = "gd-test-1";


    public S3Directory(File path) throws IOException {
        super(path);
        s3 = createClient();
        files = new HashMap<String, ByteArrayOutputStream>();
//        inputStream = s3.getObject(new GetObjectRequest(BUCKET_NAME, KEY)).getObjectContent();
//        s3.putObject(new PutObjectRequest(BUCKET_NAME, KEY, new ByteArrayInputStream(new byte[1024]), new ObjectMetadata()));
    }

    public static FSDirectory open(File path) throws IOException {
        return new S3Directory(path);
    }

    @Override
    public IndexOutput createOutput(final String name, IOContext context) throws IOException {
        final IndexOutput output = super.createOutput(name, context);

        IndexOutput indexOutput = new IndexOutput() {
            @Override
            public void flush() throws IOException {
                System.out.println("flushing - " + name);
                output.flush();
//                FileInputStream fileInputStream = new FileInputStream(new File(directory, name));
//                byte[] bytes = new byte[fileInputStream.available()];
//                fileInputStream.read(bytes);
                s3.putObject(new PutObjectRequest(BUCKET_NAME, name, new File(directory, name)));
            }

            @Override
            public void close() throws IOException {
                System.out.println("output closing - " + name);
                output.close();
                s3.putObject(new PutObjectRequest(BUCKET_NAME, name, new File(directory, name)));
            }

            @Override
            public long getFilePointer() {
                System.out.println("output filepointer- " + name);

                return output.getFilePointer();
            }

            @Override
            public void seek(long pos) throws IOException {
                System.out.println("output seek- " + name);

                output.seek(pos);
            }

            @Override
            public long length() throws IOException {
                System.out.println("output lenght- " + name);

                return output.length();
            }

            @Override
            public void writeByte(byte b) throws IOException {
                System.out.println("output writebyte- " + name);

                output.writeByte(b);
            }

            @Override
            public void writeBytes(byte[] b, int offset, int length) throws IOException {
                System.out.println("output writing with offset- " + name);
                output.writeBytes(b, offset, length);
            }
        };
        return indexOutput;
    }

    @Override
    public IndexInput openInput(final String name, IOContext context) throws IOException {
        System.out.println("name = " + name);

        S3Object object = null;
        try {
            object = s3.getObject(new GetObjectRequest(BUCKET_NAME, name));
            S3ObjectInputStream objectContent = object.getObjectContent();

            byte[] b = new byte[(int) object.getObjectMetadata().getContentLength()];
            objectContent.read(b);
            new FileOutputStream(new File(directory, name)).write(b);
        } catch (AmazonClientException e) {
            e.printStackTrace();
        }

        final IndexInput input = super.openInput(name, context);

        return new IndexInput(name) {

            @Override
            public void close() throws IOException {
                System.out.println("input close- " + name);
                input.close();
            }

            @Override
            public long getFilePointer() {
                System.out.println("input filepointer- " + name);
                return input.getFilePointer();
            }

            @Override
            public void seek(long pos) throws IOException {
                System.out.println("input seek- " + name);

                input.seek(pos);
            }

            @Override
            public long length() {
                System.out.println("input length- " + name);

                return input.length();
            }

            @Override
            public byte readByte() throws IOException {
                System.out.println("input readBytes- " + name);

                return input.readByte();
            }

            @Override
            public void readBytes(byte[] b, int offset, int len) throws IOException {
                System.out.println("input readBytes with offset- " + name);

                input.readBytes(b, offset, len);
            }
        };
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
}
