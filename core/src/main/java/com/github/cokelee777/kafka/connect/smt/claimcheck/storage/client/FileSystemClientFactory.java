package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.client;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.FileSystemStorageConfig;
import com.github.cokelee777.kafka.connect.smt.common.retry.RetryConfig;
import java.time.Duration;

/** A factory for creating {@link FileSystemClient} instances. */
public class FileSystemClientFactory {

  private static final int INITIAL_ATTEMPT = 1;

  private FileSystemClientFactory() {}

  /**
   * Creates a new {@link FileSystemClient} instance.
   *
   * @param config the configuration for the file system client
   * @return a new {@link FileSystemClient} instance
   */
  public static FileSystemClient create(FileSystemStorageConfig config) {
    return new FileSystemClient(createRetryConfig(config));
  }

  private static RetryConfig createRetryConfig(FileSystemStorageConfig config) {
    int maxAttempts = config.getRetryMax() + INITIAL_ATTEMPT;
    return new RetryConfig(
        maxAttempts,
        Duration.ofMillis(config.getRetryBackoffMs()),
        Duration.ofMillis(config.getRetryMaxBackoffMs()));
  }
}
