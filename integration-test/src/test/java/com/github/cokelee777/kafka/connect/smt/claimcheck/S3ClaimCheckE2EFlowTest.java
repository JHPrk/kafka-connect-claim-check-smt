package com.github.cokelee777.kafka.connect.smt.claimcheck;

import static org.assertj.core.api.Assertions.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.ClaimCheckSinkTransformConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.config.ClaimCheckSourceTransformConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.S3StorageConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchema;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckValue;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

@Testcontainers
class S3ClaimCheckE2EFlowTest {

  private static final String TOPIC_NAME = "test-topic";
  private static final String BUCKET_NAME = "test-bucket";
  private static final String LOCALSTACK_NETWORK_ALIAS = "localstack";

  private static Network network;

  @Container
  private static final LocalStackContainer localstack =
      new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.2.0"))
          .withServices(LocalStackContainer.Service.S3)
          .withNetwork(getNetwork())
          .withNetworkAliases(LOCALSTACK_NETWORK_ALIAS);

  @Container
  private static final ToxiproxyContainer toxiproxy =
      new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.5.0"))
          .withNetwork(getNetwork())
          .dependsOn(localstack);

  private ClaimCheckSourceTransform sourceTransform;
  private ClaimCheckSinkTransform sinkTransform;
  private static S3Client s3Client;
  private static Proxy s3Proxy;

  private static Network getNetwork() {
    if (network == null) {
      network = Network.newNetwork();
    }
    return network;
  }

  @BeforeAll
  static void beforeAll() throws IOException {
    // Initialize S3 client
    s3Client =
        S3Client.builder()
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        localstack.getAccessKey(), localstack.getSecretKey())))
            .region(Region.of(localstack.getRegion()))
            .build();

    // Create test S3 bucket
    s3Client.createBucket(builder -> builder.bucket(BUCKET_NAME));

    // Configure Toxiproxy
    ToxiproxyClient toxiproxyClient =
        new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
    s3Proxy = toxiproxyClient.createProxy("s3", "0.0.0.0:8666", LOCALSTACK_NETWORK_ALIAS + ":4566");
  }

  @AfterAll
  static void afterAll() {
    s3Client.close();
    if (network != null) {
      network.close();
    }
  }

  @BeforeEach
  void setUp() {
    sourceTransform = new ClaimCheckSourceTransform();
    sinkTransform = new ClaimCheckSinkTransform();
  }

  @AfterEach
  void tearDown() {
    sinkTransform.close();
    sourceTransform.close();
  }

  @Nested
  class NormalFlowIntegrationTest {

    @Test
    void shouldPerformClaimCheckE2EFlow() throws IOException {
      // Given: Common
      // Common config
      Map<String, Object> commonConfig = new HashMap<>();
      commonConfig.put(S3StorageConfig.BUCKET_NAME_CONFIG, BUCKET_NAME);
      commonConfig.put(S3StorageConfig.REGION_CONFIG, localstack.getRegion());
      commonConfig.put(
          S3StorageConfig.ENDPOINT_OVERRIDE_CONFIG,
          localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString());

      // Given: Source
      // ClaimCheckSourceTransform config
      Map<String, Object> sourceTransformConfig = new HashMap<>(commonConfig);
      sourceTransformConfig.put(ClaimCheckSourceTransformConfig.THRESHOLD_BYTES_CONFIG, 1);
      sourceTransformConfig.put(
          ClaimCheckSourceTransformConfig.STORAGE_TYPE_CONFIG, ClaimCheckStorageType.S3.type());
      sourceTransform.configure(sourceTransformConfig);

      SourceRecord initialSourceRecord = generateSourceRecord();

      // When: Source
      SourceRecord transformedSourceRecord = sourceTransform.apply(initialSourceRecord);

      // Then: Source
      Header transformedSourceHeader =
          validateTransformedSourceRecord(transformedSourceRecord, initialSourceRecord);

      // Given: Sink
      // ClaimCheckSinkTransform config
      Map<String, Object> sinkTransformConfig = new HashMap<>(commonConfig);
      sinkTransformConfig.put(
          ClaimCheckSinkTransformConfig.STORAGE_TYPE_CONFIG, ClaimCheckStorageType.S3.type());
      sinkTransform.configure(sinkTransformConfig);

      SinkRecord initialSinkRecord =
          generateSinkRecord(transformedSourceRecord, transformedSourceHeader);

      // When: Sink
      SinkRecord restoredSinkRecord = sinkTransform.apply(initialSinkRecord);

      // Then: Sink
      validateRestoredSinkRecord(restoredSinkRecord, initialSourceRecord);
    }
  }

  @Nested
  class S3SourceRetryIntegrationTest {

    @AfterEach
    void tearDown() throws IOException {
      clearAllToxics();
    }

    @Test
    void shouldRetryAndSucceedOnTransientFailure() throws Exception {
      // Given
      Map<String, Object> sourceTransformConfig = generateSourceConfigWithToxiproxy(3);
      sourceTransform.configure(sourceTransformConfig);

      SourceRecord initialSourceRecord = generateSourceRecord();

      // Add proxy network latency
      s3Proxy.toxics().latency("latency", ToxicDirection.DOWNSTREAM, 50);

      // When
      SourceRecord transformedRecord = sourceTransform.apply(initialSourceRecord);

      // Then
      validateTransformedSourceRecord(transformedRecord, initialSourceRecord);
    }

    @Test
    void shouldFailWhenMaxRetriesExceeded() throws Exception {
      // Given
      Map<String, Object> sourceTransformConfig = generateSourceConfigWithToxiproxy(2);
      sourceTransform.configure(sourceTransformConfig);

      SourceRecord initialSourceRecord = generateSourceRecord();

      // Completely block proxy network connection
      s3Proxy.toxics().bandwidth("bandwidth-down", ToxicDirection.DOWNSTREAM, 0);
      s3Proxy.toxics().bandwidth("bandwidth-up", ToxicDirection.UPSTREAM, 0);

      // When & Then
      assertThatThrownBy(() -> sourceTransform.apply(initialSourceRecord))
          .isInstanceOf(RuntimeException.class);
    }

    @Test
    void shouldFailImmediatelyWhenRetryDisabled() throws Exception {
      // Given
      Map<String, Object> sourceTransformConfig = generateSourceConfigWithToxiproxy(0);
      sourceTransform.configure(sourceTransformConfig);

      SourceRecord initialSourceRecord = generateSourceRecord();

      // Completely block proxy network connection
      s3Proxy.toxics().bandwidth("bandwidth-down", ToxicDirection.DOWNSTREAM, 0);
      s3Proxy.toxics().bandwidth("bandwidth-up", ToxicDirection.UPSTREAM, 0);

      // When & Then
      assertThatThrownBy(() -> sourceTransform.apply(initialSourceRecord))
          .isInstanceOf(RuntimeException.class);
    }
  }

  @Nested
  class S3SinkRetryIntegrationTest {

    @AfterEach
    void tearDown() throws IOException {
      clearAllToxics();
    }

    @Test
    void shouldRetryAndSucceedOnTransientFailure() throws Exception {
      // Given
      Map<String, Object> sinkTransformConfig = generateSinkConfigWithToxiproxy(3);
      sinkTransform.configure(sinkTransformConfig);

      SinkRecord initialSinkRecord = storeDataAndCreateSinkRecord();

      // Add proxy network latency
      s3Proxy.toxics().latency("latency", ToxicDirection.DOWNSTREAM, 50);

      // When
      SinkRecord restoredSinkRecord = sinkTransform.apply(initialSinkRecord);

      // Then
      SourceRecord initialSourceRecord = generateSourceRecord();
      validateRestoredSinkRecord(restoredSinkRecord, initialSourceRecord);
    }

    @Test
    void shouldFailWhenMaxRetriesExceeded() throws Exception {
      // Given
      SinkRecord initialSinkRecord = storeDataAndCreateSinkRecord();

      // Configure SinkTransform through Toxiproxy
      Map<String, Object> sinkTransformConfig = generateSinkConfigWithToxiproxy(2);
      sinkTransform.configure(sinkTransformConfig);

      // Completely block proxy network connection
      s3Proxy.toxics().bandwidth("bandwidth-down", ToxicDirection.DOWNSTREAM, 0);
      s3Proxy.toxics().bandwidth("bandwidth-up", ToxicDirection.UPSTREAM, 0);

      // When & Then
      assertThatThrownBy(() -> sinkTransform.apply(initialSinkRecord))
          .isInstanceOf(RuntimeException.class);
    }

    @Test
    void shouldFailImmediatelyWhenRetryDisabled() throws Exception {
      // Given
      SinkRecord initialSinkRecord = storeDataAndCreateSinkRecord();

      // Configure SinkTransform through Toxiproxy
      Map<String, Object> sinkTransformConfig = generateSinkConfigWithToxiproxy(0);
      sinkTransform.configure(sinkTransformConfig);

      // Completely block proxy network connection
      s3Proxy.toxics().bandwidth("bandwidth-down", ToxicDirection.DOWNSTREAM, 0);
      s3Proxy.toxics().bandwidth("bandwidth-up", ToxicDirection.UPSTREAM, 0);

      // When & Then
      assertThatThrownBy(() -> sinkTransform.apply(initialSinkRecord))
          .isInstanceOf(RuntimeException.class);
    }

    private SinkRecord storeDataAndCreateSinkRecord() {
      Map<String, Object> sourceConfig = new HashMap<>();
      sourceConfig.put(
          ClaimCheckSourceTransformConfig.STORAGE_TYPE_CONFIG, ClaimCheckStorageType.S3.type());
      sourceConfig.put(ClaimCheckSourceTransformConfig.THRESHOLD_BYTES_CONFIG, 1);
      sourceConfig.put(S3StorageConfig.BUCKET_NAME_CONFIG, BUCKET_NAME);
      sourceConfig.put(S3StorageConfig.REGION_CONFIG, localstack.getRegion());
      sourceConfig.put(
          S3StorageConfig.ENDPOINT_OVERRIDE_CONFIG,
          localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString());
      sourceTransform.configure(sourceConfig);

      SourceRecord initialSourceRecord = generateSourceRecord();
      SourceRecord transformedSourceRecord = sourceTransform.apply(initialSourceRecord);

      Header transformedSourceHeader =
          transformedSourceRecord.headers().lastWithName(ClaimCheckSchema.NAME);

      return generateSinkRecord(transformedSourceRecord, transformedSourceHeader);
    }
  }

  private void clearAllToxics() throws IOException {
    for (var toxic : s3Proxy.toxics().getAll()) {
      toxic.remove();
    }
  }

  private String getProxiedEndpoint() {
    return "http://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(8666);
  }

  private Map<String, Object> generateSourceConfigWithToxiproxy(int retryMax) {
    Map<String, Object> config = new HashMap<>();
    config.put(
        ClaimCheckSourceTransformConfig.STORAGE_TYPE_CONFIG, ClaimCheckStorageType.S3.type());
    config.put(ClaimCheckSourceTransformConfig.THRESHOLD_BYTES_CONFIG, 1);
    config.put(S3StorageConfig.BUCKET_NAME_CONFIG, BUCKET_NAME);
    config.put(S3StorageConfig.REGION_CONFIG, localstack.getRegion());
    config.put(S3StorageConfig.ENDPOINT_OVERRIDE_CONFIG, getProxiedEndpoint());
    config.put(S3StorageConfig.RETRY_MAX_CONFIG, retryMax);
    config.put(S3StorageConfig.RETRY_BACKOFF_MS_CONFIG, 50L);
    config.put(S3StorageConfig.RETRY_MAX_BACKOFF_MS_CONFIG, 100L);
    return config;
  }

  private Map<String, Object> generateSinkConfigWithToxiproxy(int retryMax) {
    Map<String, Object> config = new HashMap<>();
    config.put(ClaimCheckSinkTransformConfig.STORAGE_TYPE_CONFIG, ClaimCheckStorageType.S3.type());
    config.put(S3StorageConfig.BUCKET_NAME_CONFIG, BUCKET_NAME);
    config.put(S3StorageConfig.REGION_CONFIG, localstack.getRegion());
    config.put(S3StorageConfig.ENDPOINT_OVERRIDE_CONFIG, getProxiedEndpoint());
    config.put(S3StorageConfig.RETRY_MAX_CONFIG, retryMax);
    config.put(S3StorageConfig.RETRY_BACKOFF_MS_CONFIG, 50L);
    config.put(S3StorageConfig.RETRY_MAX_BACKOFF_MS_CONFIG, 100L);
    return config;
  }

  private SourceRecord generateSourceRecord() {
    Schema schema =
        SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .build();
    Struct value = new Struct(schema).put("id", 1L).put("name", "cokelee777");
    return new SourceRecord(null, null, TOPIC_NAME, null, null, schema, value);
  }

  private SinkRecord generateSinkRecord(
      SourceRecord transformedSourceRecord, Header transformedSourceHeader) {
    SinkRecord sinkRecord =
        new SinkRecord(
            transformedSourceRecord.topic(),
            0,
            transformedSourceRecord.keySchema(),
            transformedSourceRecord.key(),
            transformedSourceRecord.valueSchema(),
            transformedSourceRecord.value(),
            0);
    sinkRecord.headers().add(transformedSourceHeader);
    return sinkRecord;
  }

  private Header validateTransformedSourceRecord(
      SourceRecord transformedSourceRecord, SourceRecord initialSourceRecord) throws IOException {
    // Validate ClaimCheckSourceRecord
    assertThat(transformedSourceRecord).isNotNull();
    assertThat(transformedSourceRecord.topic()).isEqualTo(TOPIC_NAME);
    assertThat(transformedSourceRecord.keySchema()).isNull();
    assertThat(transformedSourceRecord.key()).isNull();
    assertThat(transformedSourceRecord.valueSchema()).isEqualTo(initialSourceRecord.valueSchema());
    assertThat(transformedSourceRecord.value()).isNotNull();
    assertThat(transformedSourceRecord.value()).isInstanceOf(Struct.class);
    assertThat(transformedSourceRecord.value()).isNotEqualTo(initialSourceRecord.value());

    // GenericStructStrategy applied, all fields set to default values
    assertThat(((Struct) transformedSourceRecord.value()).getInt64("id")).isEqualTo(0L);
    assertThat(((Struct) transformedSourceRecord.value()).getString("name")).isEqualTo("");

    // Validate ClaimCheckSourceHeader
    Header transformedSourceHeader =
        transformedSourceRecord.headers().lastWithName(ClaimCheckSchema.NAME);
    assertThat(transformedSourceHeader).isNotNull();
    assertThat(transformedSourceHeader.key()).isEqualTo(ClaimCheckSchema.NAME);
    assertThat(transformedSourceHeader.schema()).isEqualTo(ClaimCheckSchema.SCHEMA);
    assertThat(transformedSourceHeader.value()).isInstanceOf(Struct.class);

    // Validate actual data
    ClaimCheckValue claimCheckValue = ClaimCheckValue.from(transformedSourceHeader.value());
    String referenceUrl = claimCheckValue.referenceUrl();
    int originalSizeBytes = claimCheckValue.originalSizeBytes();

    assertThat(referenceUrl).startsWith("s3://" + BUCKET_NAME + "/");
    assertThat(originalSizeBytes).isGreaterThan(0);

    // Verify that actual data is stored in S3
    String key = referenceUrl.substring(("s3://" + BUCKET_NAME + "/").length());
    try (ResponseInputStream<GetObjectResponse> s3Object =
        s3Client.getObject(GetObjectRequest.builder().bucket(BUCKET_NAME).key(key).build())) {
      byte[] serializedRecord = s3Object.readAllBytes();
      assertThat(serializedRecord).isNotEmpty();
      assertThat(serializedRecord.length).isEqualTo(originalSizeBytes);
    }

    return transformedSourceHeader;
  }

  private void validateRestoredSinkRecord(
      SinkRecord restoredSinkRecord, SourceRecord initialSourceRecord) {
    assertThat(restoredSinkRecord).isNotNull();
    assertThat(restoredSinkRecord.topic()).isEqualTo(TOPIC_NAME);
    assertThat(restoredSinkRecord.keySchema()).isNull();
    assertThat(restoredSinkRecord.key()).isNull();
    assertThat(restoredSinkRecord.valueSchema()).isEqualTo(initialSourceRecord.valueSchema());
    assertThat(restoredSinkRecord.value()).isNotNull();
    assertThat(restoredSinkRecord.value()).isInstanceOf(Struct.class);

    // Verify that restored value equals original
    Struct restoredValue = (Struct) restoredSinkRecord.value();
    assertThat(restoredValue.getInt64("id")).isEqualTo(1L);
    assertThat(restoredValue.getString("name")).isEqualTo("cokelee777");
    assertThat(restoredValue).isEqualTo(initialSourceRecord.value());

    // Verify that ClaimCheck header is removed
    Header claimCheckSinkHeader = restoredSinkRecord.headers().lastWithName(ClaimCheckSchema.NAME);
    assertThat(claimCheckSinkHeader).isNull();
  }
}
