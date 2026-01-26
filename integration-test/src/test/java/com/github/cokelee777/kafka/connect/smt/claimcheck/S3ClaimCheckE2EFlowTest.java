package com.github.cokelee777.kafka.connect.smt.claimcheck;

import static org.assertj.core.api.Assertions.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchema;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckValue;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3.S3Storage;
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
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

@Testcontainers
@DisplayName("Claim Check SMT E2E 통합 테스트")
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
    // S3 클라이언트 초기화
    s3Client =
        S3Client.builder()
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        localstack.getAccessKey(), localstack.getSecretKey())))
            .region(Region.of(localstack.getRegion()))
            .build();

    // 테스트용 S3 버킷 생성
    s3Client.createBucket(builder -> builder.bucket(BUCKET_NAME));

    // Toxiproxy 설정
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
  void beforeEach() {
    sourceTransform = new ClaimCheckSourceTransform();
    sinkTransform = new ClaimCheckSinkTransform();
  }

  @AfterEach
  void afterEach() {
    sinkTransform.close();
    sourceTransform.close();
  }

  @Nested
  @DisplayName("정상 Flow 통합 테스트")
  class NormalFlowIntegrationTest {

    @Test
    @DisplayName("Sink -> Source 전체 흐름에서 메시지가 정상적으로 변환되고 원복되어야 한다.")
    void shouldPerformClaimCheckE2EFlow() throws IOException {
      /** Given: Common */
      // Common config
      Map<String, Object> commonConfig = new HashMap<>();
      commonConfig.put(S3Storage.Config.BUCKET_NAME, BUCKET_NAME);
      commonConfig.put(S3Storage.Config.REGION, localstack.getRegion());
      commonConfig.put(
          S3Storage.Config.ENDPOINT_OVERRIDE,
          localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString());

      /** Given: Source */
      // ClaimCheckSourceTransform config
      Map<String, Object> sourceTransformConfig = new HashMap<>(commonConfig);
      sourceTransformConfig.put(ClaimCheckSourceTransform.Config.THRESHOLD_BYTES, 1L);
      sourceTransformConfig.put(
          ClaimCheckSourceTransform.Config.STORAGE_TYPE, ClaimCheckStorageType.S3.type());
      sourceTransform.configure(sourceTransformConfig);

      SourceRecord initialSourceRecord = generateSourceRecord();

      /** When: Source */
      SourceRecord transformedSourceRecord = sourceTransform.apply(initialSourceRecord);

      /** Then: Source */
      Header transformedSourceHeader =
          validateTransformedSourceRecord(transformedSourceRecord, initialSourceRecord);

      /** Given: Sink */
      // ClaimCheckSinkTransform config
      Map<String, Object> sinkTransformConfig = new HashMap<>(commonConfig);
      sinkTransformConfig.put(
          ClaimCheckSinkTransform.Config.STORAGE_TYPE, ClaimCheckStorageType.S3.type());
      sinkTransform.configure(sinkTransformConfig);

      SinkRecord initialSinkRecord =
          generateSinkRecord(transformedSourceRecord, transformedSourceHeader);

      /** When: Sink */
      SinkRecord restoredSinkRecord = sinkTransform.apply(initialSinkRecord);

      /** Then: Sink */
      validateRestoredSinkRecord(restoredSinkRecord, initialSourceRecord);
    }
  }

  @Nested
  @DisplayName("S3 재시도 통합 테스트")
  class S3RetryIntegrationTest {

    @AfterEach
    void cleanupToxics() throws IOException {
      clearAllToxics();
    }

    private void clearAllToxics() throws IOException {
      for (var toxic : s3Proxy.toxics().getAll()) {
        toxic.remove();
      }
    }

    @Test
    @DisplayName("일시적인 네트워크 오류 발생 시 재시도하여 성공해야 한다")
    void shouldRetryAndSucceedOnTransientFailure() throws Exception {
      // Given
      Map<String, Object> sourceTransformConfig = generateConfigWithToxiproxy(3);
      sourceTransform.configure(sourceTransformConfig);

      SourceRecord initialSourceRecord = generateSourceRecord();

      // 프록시 네트워크 연결 지연
      s3Proxy.toxics().latency("latency", ToxicDirection.DOWNSTREAM, 50);

      // When
      SourceRecord transformedRecord = sourceTransform.apply(initialSourceRecord);

      // Then
      validateTransformedSourceRecord(transformedRecord, initialSourceRecord);
    }

    @Test
    @DisplayName("최대 재시도 횟수를 초과하면 예외가 발생해야 한다")
    void shouldFailWhenMaxRetriesExceeded() throws Exception {
      // Given
      Map<String, Object> sourceTransformConfig = generateConfigWithToxiproxy(2);
      sourceTransform.configure(sourceTransformConfig);

      SourceRecord initialSourceRecord = generateSourceRecord();

      // 프록시 네트워크 연결 완전 차단
      s3Proxy.toxics().bandwidth("bandwidth-down", ToxicDirection.DOWNSTREAM, 0);
      s3Proxy.toxics().bandwidth("bandwidth-up", ToxicDirection.UPSTREAM, 0);

      // When & Then
      assertThatThrownBy(() -> sourceTransform.apply(initialSourceRecord))
          .isInstanceOf(RuntimeException.class);
    }

    @Test
    @DisplayName("재시도 설정이 0일 때 즉시 실패해야 한다")
    void shouldFailImmediatelyWhenRetryDisabled() throws Exception {
      // Given
      Map<String, Object> sourceTransformConfig = generateConfigWithToxiproxy(0);
      sourceTransform.configure(sourceTransformConfig);

      SourceRecord initialSourceRecord = generateSourceRecord();

      // 프록시 네트워크 연결 완전 차단
      s3Proxy.toxics().bandwidth("bandwidth-down", ToxicDirection.DOWNSTREAM, 0);
      s3Proxy.toxics().bandwidth("bandwidth-up", ToxicDirection.UPSTREAM, 0);

      // When & Then
      assertThatThrownBy(() -> sourceTransform.apply(initialSourceRecord))
          .isInstanceOf(RuntimeException.class);
    }

    private Map<String, Object> generateConfigWithToxiproxy(int retryMax) {
      return generateConfigWithToxiproxy(retryMax, 50L, 100L);
    }

    private Map<String, Object> generateConfigWithToxiproxy(
        int retryMax, long retryBackoffMs, long retryMaxBackoffMs) {
      Map<String, Object> sourceTransformConfig = new HashMap<>();
      sourceTransformConfig.put(
          ClaimCheckSourceTransform.Config.STORAGE_TYPE, ClaimCheckStorageType.S3.type());
      sourceTransformConfig.put(ClaimCheckSourceTransform.Config.THRESHOLD_BYTES, 1L);
      sourceTransformConfig.put(S3Storage.Config.BUCKET_NAME, BUCKET_NAME);
      sourceTransformConfig.put(S3Storage.Config.REGION, localstack.getRegion());
      sourceTransformConfig.put(S3Storage.Config.ENDPOINT_OVERRIDE, getProxiedEndpoint());
      sourceTransformConfig.put(S3Storage.Config.RETRY_MAX, retryMax);
      sourceTransformConfig.put(S3Storage.Config.RETRY_BACKOFF_MS, retryBackoffMs);
      sourceTransformConfig.put(S3Storage.Config.RETRY_MAX_BACKOFF_MS, retryMaxBackoffMs);
      return sourceTransformConfig;
    }

    private String getProxiedEndpoint() {
      return "http://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(8666);
    }
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
    // ClaimCheckSourceRecord 검증
    assertThat(transformedSourceRecord).isNotNull();
    assertThat(transformedSourceRecord.topic()).isEqualTo(TOPIC_NAME);
    assertThat(transformedSourceRecord.keySchema()).isNull();
    assertThat(transformedSourceRecord.key()).isNull();
    assertThat(transformedSourceRecord.valueSchema()).isEqualTo(initialSourceRecord.valueSchema());
    assertThat(transformedSourceRecord.value()).isNotNull();
    assertThat(transformedSourceRecord.value()).isInstanceOf(Struct.class);
    assertThat(transformedSourceRecord.value()).isNotEqualTo(initialSourceRecord);

    // GenericStructStrategy 적용되어 모든 필드가 기본값으로 설정됨
    assertThat(((Struct) transformedSourceRecord.value()).getInt64("id")).isEqualTo(0L);
    assertThat(((Struct) transformedSourceRecord.value()).getString("name")).isEqualTo("");

    // ClaimCheckSourceHeader 검증
    Header transformedSourceHeader =
        transformedSourceRecord.headers().lastWithName(ClaimCheckSchema.NAME);
    assertThat(transformedSourceHeader).isNotNull();
    assertThat(transformedSourceHeader.key()).isEqualTo(ClaimCheckSchema.NAME);
    assertThat(transformedSourceHeader.schema()).isEqualTo(ClaimCheckSchema.SCHEMA);
    assertThat(transformedSourceHeader.value()).isInstanceOf(Struct.class);

    // 실제 데이터 검증
    ClaimCheckValue claimCheckValue =
        ClaimCheckValue.from((Struct) transformedSourceHeader.value());
    String referenceUrl = claimCheckValue.getReferenceUrl();
    long originalSizeBytes = claimCheckValue.getOriginalSizeBytes();

    assertThat(referenceUrl).startsWith("s3://" + BUCKET_NAME + "/");
    assertThat(originalSizeBytes).isGreaterThan(0);

    // S3에 실제 데이터가 저장되었는지 확인
    String key = referenceUrl.substring(("s3://" + BUCKET_NAME + "/").length());
    byte[] serializedRecord =
        s3Client
            .getObject(GetObjectRequest.builder().bucket(BUCKET_NAME).key(key).build())
            .readAllBytes();
    assertThat(serializedRecord).isNotEmpty();
    assertThat(serializedRecord.length).isEqualTo(originalSizeBytes);
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

    // 복원된 값이 원본과 동일한지 검증
    Struct restoredValue = (Struct) restoredSinkRecord.value();
    assertThat(restoredValue.getInt64("id")).isEqualTo(1L);
    assertThat(restoredValue.getString("name")).isEqualTo("cokelee777");
    assertThat(restoredValue).isEqualTo(initialSourceRecord.value());

    // ClaimCheck 헤더가 제거되었는지 확인
    Header claimCheckSinkHeader = restoredSinkRecord.headers().lastWithName(ClaimCheckSchema.NAME);
    assertThat(claimCheckSinkHeader).isNull();
  }
}
