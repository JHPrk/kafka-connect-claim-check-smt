package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.S3StorageConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.S3StorageTestConfigProvider;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

@ExtendWith(MockitoExtension.class)
class S3StorageTest {

  @InjectMocks private S3Storage s3Storage;
  @Mock private S3Client s3Client;

  @Nested
  class ConfigureTest {

    @Test
    void shouldConfigureWithAllProvidedArguments() {
      // Given
      Map<String, String> configs =
          S3StorageTestConfigProvider.config(
              "test-bucket", Region.AP_NORTHEAST_1.id(), "test/path", null, 5, 500L, 30000L);

      // When
      s3Storage.configure(configs);

      // Then
      assertThat(s3Storage.getConfig().getBucketName()).isEqualTo("test-bucket");
      assertThat(s3Storage.getConfig().getRegion()).isEqualTo(Region.AP_NORTHEAST_1.id());
      assertThat(s3Storage.getConfig().getPathPrefix()).isEqualTo("test/path");
      assertThat(s3Storage.getConfig().getRetryMax()).isEqualTo(5);
      assertThat(s3Storage.getConfig().getRetryBackoffMs()).isEqualTo(500L);
      assertThat(s3Storage.getConfig().getRetryMaxBackoffMs()).isEqualTo(30000L);
    }

    @Test
    void shouldUseDefaultValuesWhenOnlyBucketNameProvided() {
      // Given
      Map<String, String> configs = S3StorageTestConfigProvider.config("test-bucket");

      // When
      s3Storage.configure(configs);

      // Then
      assertThat(s3Storage.getConfig().getBucketName()).isEqualTo("test-bucket");
      assertThat(s3Storage.getConfig().getRegion()).isEqualTo(S3StorageConfig.REGION_DEFAULT);
      assertThat(s3Storage.getConfig().getPathPrefix())
          .isEqualTo(S3StorageConfig.PATH_PREFIX_DEFAULT);
      assertThat(s3Storage.getConfig().getRetryMax()).isEqualTo(S3StorageConfig.RETRY_MAX_DEFAULT);
      assertThat(s3Storage.getConfig().getRetryBackoffMs())
          .isEqualTo(S3StorageConfig.RETRY_BACKOFF_MS_DEFAULT);
      assertThat(s3Storage.getConfig().getRetryMaxBackoffMs())
          .isEqualTo(S3StorageConfig.RETRY_MAX_BACKOFF_MS_DEFAULT);
    }

    @Test
    void shouldThrowExceptionWhenBucketNameIsMissing() {
      // Given
      Map<String, String> configs = S3StorageTestConfigProvider.config();

      // When & Then
      assertThatExceptionOfType(ConfigException.class)
          .isThrownBy(() -> s3Storage.configure(configs))
          .withMessage(
              "Missing required configuration \"storage.s3.bucket.name\" which has no default value.");
    }
  }

  @Nested
  class StoreTest {

    @BeforeEach
    void setUp() {
      Map<String, String> configs = S3StorageTestConfigProvider.config("test-bucket");
      s3Storage.configure(configs);
    }

    @Test
    void shouldStorePayloadAndReturnS3ReferenceUrl() {
      // Given
      byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);

      // When
      String referenceUrl = s3Storage.store(payload);

      // Then
      verify(s3Client, times(1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
      assertThat(referenceUrl).isNotBlank();
      assertThat(referenceUrl).startsWith("s3://");
    }

    @Test
    void shouldThrowRuntimeExceptionWhenS3UploadFails() {
      // Given
      byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
      when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
          .thenThrow(S3Exception.builder().message("Test S3 Exception").build());

      // When & Then
      assertThatExceptionOfType(RuntimeException.class)
          .isThrownBy(() -> s3Storage.store(payload))
          .withMessageStartingWith("Failed to upload to S3. Bucket: test-bucket");
    }
  }

  @Nested
  class CloseTest {

    @Test
    void shouldCloseS3Client() {
      // Given & When
      s3Storage.close();

      // Then
      verify(s3Client, times(1)).close();
    }

    @Test
    void shouldNotThrowExceptionWhenS3ClientIsNull() {
      // Given
      s3Storage = new S3Storage();

      // When & Then
      assertDoesNotThrow(() -> s3Storage.close());
    }
  }
}
