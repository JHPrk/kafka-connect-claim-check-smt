package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type.S3Storage;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/** Configuration for creating an S3 client. */
public final class S3ClientConfig {

  private record Properties(
      String region,
      String endpointOverride,
      int retryMax,
      long retryBackoffMs,
      long retryMaxBackoffMs) {}

  private final Properties properties;

  private S3ClientConfig(Properties properties) {
    this.properties = properties;
  }

  /**
   * Creates an S3ClientConfig from a Kafka Connect SimpleConfig.
   *
   * @param config the Kafka Connect configuration containing: - region: AWS region -
   *     endpointOverride: optional endpoint override for testing - retryMax: maximum retry attempts
   *     - retryBackoffMs: initial backoff in milliseconds - retryMaxBackoffMs: maximum backoff in
   *     milliseconds
   * @return a new S3ClientConfig instance
   */
  public static S3ClientConfig from(SimpleConfig config) {
    String region = config.getString(S3Storage.Config.REGION);
    String endpointOverride = config.getString(S3Storage.Config.ENDPOINT_OVERRIDE);
    int retryMax = config.getInt(S3Storage.Config.RETRY_MAX);
    long retryBackoffMs = config.getLong(S3Storage.Config.RETRY_BACKOFF_MS);
    long retryMaxBackoffMs = config.getLong(S3Storage.Config.RETRY_MAX_BACKOFF_MS);

    Properties properties =
        new Properties(region, endpointOverride, retryMax, retryBackoffMs, retryMaxBackoffMs);
    return new S3ClientConfig(properties);
  }

  /** Returns the AWS region. */
  public String region() {
    return properties.region();
  }

  /** Returns the endpoint override URL, or {@code null} if not set. */
  public String endpointOverride() {
    return properties.endpointOverride();
  }

  /** Returns the maximum number of retry attempts. */
  public int retryMax() {
    return properties.retryMax();
  }

  /** Returns the initial backoff in milliseconds. */
  public long retryBackoffMs() {
    return properties.retryBackoffMs();
  }

  /** Returns the maximum backoff in milliseconds. */
  public long retryMaxBackoffMs() {
    return properties.retryMaxBackoffMs();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    S3ClientConfig that = (S3ClientConfig) o;

    // 내부 record의 equals() 결과에 위임
    return this.properties.equals(that.properties);
  }

  @Override
  public int hashCode() {
    return this.properties.hashCode();
  }

  @Override
  public String toString() {
    return "S3ClientConfig[" + this.properties.toString() + "]";
  }
}
