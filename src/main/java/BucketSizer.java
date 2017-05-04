import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * Created by cwikj on 8/26/15.
 */
public class BucketSizer {

    public static final int EXIT_OK = 0;
    public static final int EXIT_CREATE_FILE_ERROR = 1;
    public static final int EXIT_WRITE_FILE_ERROR = 2;
    public static final int EXIT_CLOSE_FILE_ERROR = 3;

    public static final int NEW_LINE = 0x0a;

    public static final String OPT_ENDPOINT = "endpoint";
    public static final String OPT_ACCESS_KEY = "access-key";
    public static final String OPT_SECRET_KEY = "secret-key";
    public static final String OPT_BUCKET = "bucket";
    public static final String OPT_PAGE_SIZE = "page-size";
    public static final String OPT_SOCKET_TIMEOUT = "socket-timeout";
    public static final String OPT_NO_VERSIONS = "no-versions";
    public static final String OPT_KEY_MARKER = "key-marker";
    public static final String OPT_VERSION_MARKER = "version-marker";
    public static final String OPT_KEY_FILE = "key-file";

    public static void main(String[] args) {
        Options opts = buildOptions();

        DefaultParser dp = new DefaultParser();
        CommandLine cli = null;
        try {
            cli = dp.parse(opts, args);
        } catch (ParseException e) {
            System.err.println("Error: " + e);
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp("java -jar bucket-sizer-{version}.jar", opts);
            System.exit(255);
        }

        URI endpoint = URI.create(cli.getOptionValue(OPT_ENDPOINT));
        boolean isVhost = false;

        String accessKey = cli.getOptionValue(OPT_ACCESS_KEY);
        String secret = cli.getOptionValue(OPT_SECRET_KEY);
        String bucketName = cli.getOptionValue(OPT_BUCKET);
        int timeout = -1;
        boolean noVersions = false;
        String keyFile = null;
        int pageSize = 1000;
        String keyMarker = null;
        String versionMarker = null;

        if(cli.hasOption(OPT_KEY_FILE)) {
            keyFile = cli.getOptionValue(OPT_KEY_FILE);
        }
        if(cli.hasOption(OPT_NO_VERSIONS)) {
            noVersions = true;
        }
        if(cli.hasOption(OPT_SOCKET_TIMEOUT)) {
            timeout = Integer.parseInt(cli.getOptionValue(OPT_SOCKET_TIMEOUT));
        }
        if(cli.hasOption(OPT_PAGE_SIZE)) {
            pageSize = Integer.parseInt(cli.getOptionValue(OPT_PAGE_SIZE));
        }
        if(cli.hasOption(OPT_KEY_MARKER)) {
            keyMarker = cli.getOptionValue(OPT_KEY_MARKER);
        }
        if(cli.hasOption(OPT_VERSION_MARKER)) {
            versionMarker = cli.getOptionValue(OPT_VERSION_MARKER);
        }

        ClientConfiguration config = new ClientConfiguration();
        // Force use of v2 Signer.  ECS does not support v4 signatures yet.
        config.setSignerOverride("S3SignerType");

        if(timeout != -1) {
            config.setSocketTimeout(timeout);
        }

        AmazonS3Client s3 = new AmazonS3Client(new BasicAWSCredentials(accessKey, secret), config);

        // required only if using path-style buckets
        S3ClientOptions options = new S3ClientOptions();
        options.setPathStyleAccess(true);
        s3.setS3ClientOptions(options);
        s3.setEndpoint(endpoint.toString());

        if(noVersions) {
            listObjects(s3, bucketName, pageSize, keyMarker, keyFile);
        } else {
            listVersions(s3, bucketName, pageSize, keyMarker, versionMarker, keyFile);
        }

       System.exit(EXIT_OK);
    }

    private static Options buildOptions() {
        Options opts = new Options();

        opts.addOption(Option.builder().longOpt(OPT_ACCESS_KEY).hasArg().argName("access key").desc("The S3 access key (ECS Object User)").required().build());
        opts.addOption(Option.builder().longOpt(OPT_SECRET_KEY).hasArg().argName("secret key").desc("The secret key for the access key").required().build());
        opts.addOption(Option.builder().longOpt(OPT_ENDPOINT).hasArg().argName("URL").desc("The S3 endpoint, including scheme and hostname. Port optional.").required().build());
        opts.addOption(Option.builder().longOpt(OPT_BUCKET).hasArg().argName("bucket name").desc("The bucket to enumerate").required().build());
        opts.addOption(Option.builder().longOpt(OPT_NO_VERSIONS).desc("If specified, do not enumerate versions").build());
        opts.addOption(Option.builder().longOpt(OPT_PAGE_SIZE).hasArg().argName("items").desc("Change the number of items per page").build());
        opts.addOption(Option.builder().longOpt(OPT_SOCKET_TIMEOUT).hasArg().argName("milliseconds").desc("Sets the socket timeout in ms.").build());
        opts.addOption(Option.builder().longOpt(OPT_KEY_MARKER).hasArg().argName("key").desc("Key marker to resume listing from").build());
        opts.addOption(Option.builder().longOpt(OPT_VERSION_MARKER).hasArg().argName("version-id").desc("Version marker to resume the listing from").build());
        opts.addOption(Option.builder().longOpt(OPT_KEY_FILE).hasArg().argName("key file").desc("If specified, the file path to write key names").build());

        return opts;
    }

    private static void listObjects(AmazonS3Client s3, String bucketName, int pageSize, String keyMarker, String keyFile) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long sum = 0;
        long count = 0;
        long start = System.currentTimeMillis();
        String lastKey = null;
        FileOutputStream out = null;

        if (keyFile != null) {
            out = getFileOutputStream(keyFile);
        }

        ListObjectsRequest req = new ListObjectsRequest().withBucketName(bucketName).withMaxKeys(pageSize);
        if (keyMarker != null) {
            req.setMarker(keyMarker);
        }

        do {
            System.out.printf("%d items processed\n", count);

            ObjectListing res = s3.listObjects(req);

            for (S3ObjectSummary obj : res.getObjectSummaries()) {
                long sz = obj.getSize();
                sum += sz;
                count++;
                min = Math.min(sz, min);
                max = Math.max(sz, max);
                lastKey = obj.getKey();
                if (out != null) {
                    writeKeyToFile(out, lastKey);
                }
            }

            if (res.isTruncated()) {
                if (res.getNextMarker() != null) {
                    req.setMarker(res.getNextMarker());
                } else {
                    // use last key
                    req.setMarker(lastKey);
                }
            } else {
                req.setMarker(null);
            }
            System.out.printf("IsTruncated? %s NextMarker: %s\n", res.isTruncated(), res.getNextMarker());

        } while (req.getMarker() != null);

        if (count == 0L) {
            System.out.printf("Item count: %d  Total Size: %s\n", 0, 0);
        } else {
            System.out.printf("------------------------\n");
            long elapsed = System.currentTimeMillis() - start;
            System.out.printf("Processed %d items in %dms (%d items/s)\n", count, elapsed, (count / (elapsed / 1000)));
            System.out.printf("Item count: %d  Total Size: %s\n", count, toHuman(sum));
            System.out.printf("Min: %s  Max: %s Mean: %s\n", toHuman(min), toHuman(max), toHuman(sum / count));
        }

        // out file
        if (out != null) {
            closeOutputFileStream(out);
            System.out.printf("Wrote key names to file: %s\n", keyFile);
        }
    }

    private static void listVersions(AmazonS3Client s3, String bucketName, int pageSize, String keyMarker, String versionMarker, String keyFile) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long sum = 0;
        long noncurrentSum = 0;
        long count = 0;
        long noncurrentCount = 0;
        long deleteMarkers = 0;
        long start = System.currentTimeMillis();
        String lastKey = null;
        FileOutputStream out = null;

        if (keyFile != null) {
            out = getFileOutputStream(keyFile);
        }

        ListVersionsRequest lvr = new ListVersionsRequest().withBucketName(bucketName).withMaxResults(pageSize);
        if (keyMarker != null) {
            lvr.setKeyMarker(keyMarker);
        }
        if (versionMarker != null) {
            lvr.setVersionIdMarker(versionMarker);
        }

        do {
            System.out.printf("%d items processed\n", count);

            VersionListing res = s3.listVersions(lvr);

            for (S3VersionSummary obj : res.getVersionSummaries()) {
                long sz = obj.getSize();
                sum += sz;
                count++;
                min = Math.min(sz, min);
                max = Math.max(sz, max);
                lastKey = obj.getKey() + "?versionId=" + obj.getVersionId();

                if (obj.isDeleteMarker()) {
                    deleteMarkers++;
                }
                if (obj.isDeleteMarker() || !obj.isLatest()) {
                    noncurrentCount++;
                    noncurrentSum += sz;
                }
                if (out != null) {
                    writeKeyToFile(out, lastKey);
                }
            }

            System.out.printf("IsTruncated? %s NextKeyMarker: %s NextVersionMarker: %s\n", res.isTruncated(), res.getNextKeyMarker(), res.getNextVersionIdMarker());

            if (res.isTruncated()) {
                lvr.setKeyMarker(res.getNextKeyMarker());
                lvr.setVersionIdMarker(res.getNextVersionIdMarker());
            } else {
                lvr.setKeyMarker(null);
            }
        } while (lvr.getKeyMarker() != null);

        if (count == 0L) {
            System.out.printf("Item count: %d  Total Size: %s\n", 0, 0);
        } else {
            System.out.printf("------------------------\n");
            long elapsed = System.currentTimeMillis() - start;
            System.out.printf("Processed %d items in %dms (%d items/s)\n", count, elapsed, (count / (elapsed / 1000)));
            System.out.printf("Item count: %d  Total Size: %s\n", count, toHuman(sum));
            System.out.printf("Min: %s  Max: %s Mean: %s\n", toHuman(min), toHuman(max), toHuman(sum / count));

            // Versions
            if (noncurrentCount == 0L) {
                System.out.printf("No versions found in bucket\n");
            } else {
                System.out.printf("Found %d non-current versions (%d delete markers)\n", noncurrentCount, deleteMarkers);
                System.out.printf("Current versions: %s Non-current versions: %s\n", toHuman(sum - noncurrentSum),
                        toHuman(noncurrentSum));
            }
        }

        // out file
        if (out != null) {
            closeOutputFileStream(out);
            System.out.printf("Wrote key names (with version id) to file: %s\n", keyFile);
        }
    }

    private static FileOutputStream getFileOutputStream(String file) {
        FileOutputStream fileOutputStream = null;
        try {
            fileOutputStream = new FileOutputStream(new File(file));
        } catch (Exception e) {
            System.out.printf("error creating output file");
            System.exit(EXIT_CREATE_FILE_ERROR);
        }
        return fileOutputStream;
    }

    private static void writeKeyToFile(FileOutputStream out, String key) {
        try {
            out.write(key.getBytes());
            out.write(NEW_LINE);
        } catch (Exception e) {
            System.out.printf("error writing key name to output file");
            System.exit(EXIT_WRITE_FILE_ERROR);
        }
    }

    private static void closeOutputFileStream(FileOutputStream out) {
        try {
            out.close();
        } catch (Exception e) {
            System.out.printf("error closing output file");
            System.exit(EXIT_CLOSE_FILE_ERROR);
        }
    }

    private static final double KIBIBYTE = 1024.0;
    private static final double MIBIBYTE = 1048576.0;
    private static final double GIBIBYTE = 1073741824.0;
    private static final double TIBIBYTE = 1099511627776.0;

    private static String toHuman(double v) {
        if(v < KIBIBYTE) {
            return String.format("%.3f B", v);
        } else if(v < MIBIBYTE) {
            return String.format("%.3f KiB", v/KIBIBYTE);
        } else if(v < GIBIBYTE) {
            return String.format("%.3f MiB", v/MIBIBYTE);
        } else if(v < TIBIBYTE) {
            return String.format("%.3f GiB", v/GIBIBYTE);
        } else {
            return String.format("%.3f TiB", v/TIBIBYTE);
        }
    }
}
