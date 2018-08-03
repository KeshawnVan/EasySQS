import com.amazonaws.regions.Regions;

public class SqsConfig {

    private static final SqsConfig SQS_AUTHORIZATION_INFO = new SqsConfig();

    private String accessKeyId;

    private String secretKey;

    private String region;

    private String queuePrefix;

    public SqsConfig() {
        String zkAccessKeyId = "";
        String zkSecretKey = "";
        String zkRegion = Regions.US_WEST_2.getName();
        String zkQueuePrefix = "dev-";

        this.accessKeyId = zkAccessKeyId;
        this.secretKey = zkSecretKey;
        this.region = zkRegion;
        this.queuePrefix = zkQueuePrefix;
    }

    public static SqsConfig getInstance() {
        return SQS_AUTHORIZATION_INFO;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public void setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getQueuePrefix() {
        return queuePrefix;
    }

    public void setQueuePrefix(String queuePrefix) {
        this.queuePrefix = queuePrefix;
    }
}
