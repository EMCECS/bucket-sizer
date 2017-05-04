import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;

import java.io.ByteArrayInputStream;
import java.net.URI;

/**
 * Created by cwikj on 8/26/15.
 */
public class PartNumberTest {

    public static void main(String[] args) {
        if(args.length != 4) {
            System.err.printf("Usage:\n  java -jar BucketSizer.jar {endpoint} {accessKey} {secretKey} {bucketName}\n");
            System.exit(1);
        }

        URI endpoint = URI.create(args[0]);
        boolean isVhost = endpoint.getHost().contains("ecstestdrive");
        String accessKey = args[1];
        String secret = args[2];
        String bucketName = args[3];
        String keyName = "uploadtest.bin";
//
//        S3Config config = null;
//        if(isVhost) {
//            config = new S3Config(endpoint);
//        } else {
//            config = new S3Config(Protocol.valueOf(endpoint.getScheme().toUpperCase()), endpoint.getHost());
//            if (endpoint.getPort() != -1) {
//                config.setPort(endpoint.getPort());
//            }
//        }
//        config.withIdentity(accessKey).withSecretKey(secret);
//
//        S3Client s3 = new S3JerseyClient(config);

        ClientConfiguration config = new ClientConfiguration();
        // Force use of v2 Signer.  ECS does not support v4 signatures yet.
        config.setSignerOverride("S3SignerType");

        AmazonS3Client s3 = new AmazonS3Client(new BasicAWSCredentials(accessKey, secret), config);

        // required only if using path-style buckets
        S3ClientOptions options = new S3ClientOptions();
        options.setPathStyleAccess(true);
        s3.setS3ClientOptions(options);
        s3.setEndpoint(endpoint.toString());

        InitiateMultipartUploadRequest mpur = new InitiateMultipartUploadRequest(bucketName, keyName);
        InitiateMultipartUploadResult mpures = s3.initiateMultipartUpload(mpur);
        String uploadId = mpures.getUploadId();
        System.out.printf("Initiated multipart upload ID %s\n", uploadId);

        // Upload some parts
        byte[] data = new byte[102400];

        for(int i=1; i<11; i++) {
            UploadPartRequest upr = new UploadPartRequest();
            upr.withBucketName(bucketName).withKey(keyName).withUploadId(uploadId).withPartNumber(i)
                    .withPartSize(102400)
                    .withInputStream(new ByteArrayInputStream(data));
            UploadPartResult partResult = s3.uploadPart(upr);
            System.out.printf("Uploaded part %d\n", partResult.getPartNumber());
        }

        try {
            System.out.printf("Listing parts starting at 2\n");
            ListPartsRequest lpr = new ListPartsRequest(bucketName, keyName, uploadId).withPartNumberMarker(2);
            PartListing partListing = s3.listParts(lpr);
            System.out.printf("Got %d parts back\n", partListing.getParts().size());

        } finally {
            AbortMultipartUploadRequest ampu = new AbortMultipartUploadRequest(bucketName, keyName, uploadId);
            System.out.printf("Canceling uploadId %s...", uploadId);
            s3.abortMultipartUpload(ampu);
            System.out.printf("..done\n");
        }

    }
}
