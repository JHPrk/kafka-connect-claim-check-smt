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

  public static Map<String, String> config() {
    return new HashMap<>();
  }

  public static Map<String, String> config(String bucketName) {
    Map<String, String> configs = config();
    configs.put(BUCKET_NAME_CONFIG, bucketName);
    return configs;
  }

  public static Map<String, String> config(String bucketName, String region) {
    Map<String, String> configs = config(bucketName);
    configs.put(REGION_CONFIG, region);
    return configs;
  }

  public static Map<String, String> config(String bucketName, String region, String pathPrefix) {
    Map<String, String> configs = config(bucketName, region);
    configs.put(PATH_PREFIX_CONFIG, pathPrefix);
    return configs;
  }

  public static Map<String, String> config(
      String bucketName, String region, String pathPrefix, String endpointOverride) {
    Map<String, String> configs = config(bucketName, region, pathPrefix);
    configs.put(ENDPOINT_OVERRIDE_CONFIG, endpointOverride);
    return configs;
  }

  public static Map<String, String> config(
      String bucketName, String region, String pathPrefix, String endpointOverride, int retryMax) {
    Map<String, String> configs = config(bucketName, region, pathPrefix, endpointOverride);
    configs.put(RETRY_MAX_CONFIG, String.valueOf(retryMax));
    return configs;
  }

  public static Map<String, String> config(
      String bucketName,
      String region,
      String pathPrefix,
      String endpointOverride,
      int retryMax,
      long retryBackoffMs) {
    Map<String, String> configs =
        config(bucketName, region, pathPrefix, endpointOverride, retryMax);
    configs.put(RETRY_BACKOFF_MS_CONFIG, String.valueOf(retryBackoffMs));
    return configs;
  }

  public static Map<String, String> config(
      String bucketName,
      String region,
      String pathPrefix,
      String endpointOverride,
      int retryMax,
      long retryBackoffMs,
      long retryMaxBackoffMs) {
    Map<String, String> configs =
        config(bucketName, region, pathPrefix, endpointOverride, retryMax, retryBackoffMs);
    configs.put(RETRY_MAX_BACKOFF_MS_CONFIG, String.valueOf(retryMaxBackoffMs));
    return configs;
  }

  public static Map<String, String> configWithEndpointOverride(String bucketName) {
    Map<String, String> configs = config(bucketName);
    configs.put(ENDPOINT_OVERRIDE_CONFIG, ENDPOINT_OVERRIDE_DEFAULT);
    return configs;
  }

  public static Map<String, String> configWithEndpointOverride(String bucketName, String region) {
    Map<String, String> configs = configWithEndpointOverride(bucketName);
    configs.put(REGION_CONFIG, region);
    return configs;
  }

  public static Map<String, String> configWithEndpointOverride(
      String bucketName, String region, String pathPrefix) {
    Map<String, String> configs = configWithEndpointOverride(bucketName, region);
    configs.put(PATH_PREFIX_CONFIG, pathPrefix);
    return configs;
  }

  public static Map<String, String> configWithEndpointOverride(
      String bucketName, String region, String pathPrefix, int retryMax) {
    Map<String, String> configs = configWithEndpointOverride(bucketName, region, pathPrefix);
    configs.put(RETRY_MAX_CONFIG, String.valueOf(retryMax));
    return configs;
  }

  public static Map<String, String> configWithEndpointOverride(
      String bucketName, String region, String pathPrefix, int retryMax, long retryBackoffMs) {
    Map<String, String> configs =
        configWithEndpointOverride(bucketName, region, pathPrefix, retryMax);
    configs.put(RETRY_BACKOFF_MS_CONFIG, String.valueOf(retryBackoffMs));
    return configs;
  }

  public static Map<String, String> configWithEndpointOverride(
      String bucketName,
      String region,
      String pathPrefix,
      int retryMax,
      long retryBackoffMs,
      long retryMaxBackoffMs) {
    Map<String, String> configs =
        configWithEndpointOverride(bucketName, region, pathPrefix, retryMax, retryBackoffMs);
    configs.put(RETRY_MAX_BACKOFF_MS_CONFIG, String.valueOf(retryMaxBackoffMs));
    return configs;
  }
}
