package com.leonardozv.spark.connectors.aws.dynamodb.write;

import software.amazon.awssdk.regions.Region;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DynamoDbSinkOptions implements Serializable {

    private final Map<String, String> options;

    public DynamoDbSinkOptions(Map<String, String> options) {
        this.options = new HashMap<>(options);
    }

    public String credentialsProvider() {
        return this.options.computeIfAbsent("credentialsProvider", k -> "DefaultCredentialsProvider");
    }

    public String profile() {
        return this.options.computeIfAbsent("profile", k -> "");
    }

    public String accessKeyId() {
        return this.options.computeIfAbsent("accessKeyId", k -> "");
    }

    public String secretAccessKey() {
        return this.options.computeIfAbsent("secretAccessKey", k -> "");
    }

    public String sessionToken() {
        return this.options.computeIfAbsent("sessionToken", k -> "");
    }

    public String endpoint() {
        return this.options.computeIfAbsent("endpoint", k -> "");
    }

    public Region region() {
        return Region.of(this.options.computeIfAbsent("region", k -> "us-east-1"));
    }

    public int batchSize() {
        return Integer.parseInt(this.options.computeIfAbsent("batchSize", k -> "25"));
    }

    public Set<String> errorsToIgnore() {
        return Arrays.stream(this.options.computeIfAbsent("errorsToIgnore", k -> "").split(",")).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
    }

}
