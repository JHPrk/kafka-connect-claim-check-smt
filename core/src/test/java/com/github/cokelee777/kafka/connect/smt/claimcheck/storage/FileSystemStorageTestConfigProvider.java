package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.FileSystemStorageConfig.PATH_CONFIG;
import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.FileSystemStorageConfig.RETRY_BACKOFF_MS_CONFIG;
import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.FileSystemStorageConfig.RETRY_MAX_BACKOFF_MS_CONFIG;
import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.FileSystemStorageConfig.RETRY_MAX_CONFIG;

import java.util.HashMap;
import java.util.Map;

public class FileSystemStorageTestConfigProvider {

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final Map<String, String> configs = new HashMap<>();

    public Builder path(String path) {
      configs.put(PATH_CONFIG, path);
      return this;
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
