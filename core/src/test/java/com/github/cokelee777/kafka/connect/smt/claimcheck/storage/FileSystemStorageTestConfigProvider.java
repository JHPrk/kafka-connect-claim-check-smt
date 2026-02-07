package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.FileSystemStorageConfig.PATH_CONFIG;
import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.FileSystemStorageConfig.RETRY_BACKOFF_MS_CONFIG;
import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.FileSystemStorageConfig.RETRY_MAX_BACKOFF_MS_CONFIG;
import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.FileSystemStorageConfig.RETRY_MAX_CONFIG;

import java.util.HashMap;
import java.util.Map;

public class FileSystemStorageTestConfigProvider {

  public static Map<String, String> config() {
    return new HashMap<>();
  }

  public static Map<String, String> config(String path) {
    Map<String, String> configs = config();
    configs.put(PATH_CONFIG, path);
    return configs;
  }

  public static Map<String, String> config(String path, int retryMax) {
    Map<String, String> configs = config(path);
    configs.put(RETRY_MAX_CONFIG, String.valueOf(retryMax));
    return configs;
  }

  public static Map<String, String> config(String path, int retryMax, long retryBackoffMs) {
    Map<String, String> configs = config(path, retryMax);
    configs.put(RETRY_BACKOFF_MS_CONFIG, String.valueOf(retryBackoffMs));
    return configs;
  }

  public static Map<String, String> config(
      String path, int retryMax, long retryBackoffMs, long retryMaxBackoffMs) {
    Map<String, String> configs = config(path, retryMax, retryBackoffMs);
    configs.put(RETRY_MAX_BACKOFF_MS_CONFIG, String.valueOf(retryMaxBackoffMs));
    return configs;
  }
}
