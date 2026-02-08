package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.S3StorageConfig.BUCKET_NAME_CONFIG;
import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.S3StorageConfig.ENDPOINT_OVERRIDE_CONFIG;
import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.S3StorageConfig.PATH_PREFIX_CONFIG;
import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.S3StorageConfig.REGION_CONFIG;
import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.S3StorageConfig.RETRY_BACKOFF_MS_CONFIG;
import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.S3StorageConfig.RETRY_MAX_BACKOFF_MS_CONFIG;
import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.S3StorageConfig.RETRY_MAX_CONFIG;

import java.util.HashMap;
import java.util.Map;

public class S3StorageTestConfigProvider {

  public static final String ENDPOINT_OVERRIDE_DEFAULT = "http://localhost:4566";

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final Map<String, String> configs = new HashMap<>();

    public Builder bucketName(String bucketName) {
      configs.put(BUCKET_NAME_CONFIG, bucketName);
      return this;
    }

    public Builder region(String region) {
      configs.put(REGION_CONFIG, region);
      return this;
    }

    public Builder pathPrefix(String pathPrefix) {
      configs.put(PATH_PREFIX_CONFIG, pathPrefix);
      return this;
    }

    public Builder endpointOverride(String endpointOverride) {
      configs.put(ENDPOINT_OVERRIDE_CONFIG, endpointOverride);
      return this;
    }

    public Builder withDefaultEndpointOverride() {
      return endpointOverride(ENDPOINT_OVERRIDE_DEFAULT);
    }

    public Builder retryMax(int retryMax) {
      configs.put(RETRY_MAX_CONFIG, String.valueOf(retryMax));
      return this;
    }

    public Builder retryBackoffMs(long retryBackoffMs) {
      configs.put(RETRY_BACKOFF_MS_CONFIG, String.valueOf(retryBackoffMs));
      return this;
    }

    public Builder retryMaxBackoffMs(long retryMaxBackoffMs) {
      configs.put(RETRY_MAX_BACKOFF_MS_CONFIG, String.valueOf(retryMaxBackoffMs));
      return this;
    }

    public Map<String, String> build() {
      return new HashMap<>(configs);
    }
  }
}
