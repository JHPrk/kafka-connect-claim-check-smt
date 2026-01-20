package com.github.cokelee777.kafka.connect.smt.claimcheck.model;

import java.time.Instant;
import java.util.Objects;
import org.apache.kafka.connect.data.Struct;

/**
 * Represents a "claim check" reference to a payload stored in an external system.
 *
 * <p>This object contains the location of the stored data (URL), its original size, and the
 * timestamp of when it was uploaded.
 */
public class ClaimCheckReference {

  private final String referenceUrl;
  private final long originalSizeBytes;
  private final long uploadedAt;
  private final boolean originalSchemasEnabled;
  private final String originalSchemaJson;
  private final RecordValueType originalRecordValueType;

  private ClaimCheckReference(
      String referenceUrl,
      long originalSizeBytes,
      long uploadedAt,
      boolean originalSchemasEnabled,
      String originalSchemaJson,
      RecordValueType originalRecordValueType) {
    this.referenceUrl = referenceUrl;
    this.originalSizeBytes = originalSizeBytes;
    this.uploadedAt = uploadedAt;
    this.originalSchemasEnabled = originalSchemasEnabled;
    this.originalSchemaJson = originalSchemaJson;
    this.originalRecordValueType = originalRecordValueType;
  }

  /**
   * Creates a new {@link ClaimCheckReference} with the current timestamp.
   *
   * @param referenceUrl The URL pointing to the stored payload.
   * @param originalSizeBytes The original size of the payload in bytes.
   * @param originalSchemasEnabled Whether the original record used schemas.
   * @param originalSchemaJson The serialized schema JSON, or {@code null} if schemas were not
   *     enabled.
   * @param originalRecordValueType The type of the original value.
   * @return A new {@link ClaimCheckReference} instance.
   */
  public static ClaimCheckReference create(
      String referenceUrl,
      long originalSizeBytes,
      boolean originalSchemasEnabled,
      String originalSchemaJson,
      RecordValueType originalRecordValueType) {
    if (referenceUrl == null || referenceUrl.isBlank()) {
      throw new IllegalArgumentException("referenceUrl must be non-blank");
    }
    if (originalSizeBytes < 0) {
      throw new IllegalArgumentException("originalSizeBytes must be >= 0");
    }
    Objects.requireNonNull(originalRecordValueType, "originalValueType must not be null");
    return new ClaimCheckReference(
        referenceUrl,
        originalSizeBytes,
        Instant.now().toEpochMilli(),
        originalSchemasEnabled,
        originalSchemaJson,
            originalRecordValueType);
  }

  /**
   * Converts this reference object into a Kafka Connect {@link Struct} using the {@link
   * ClaimCheckSchema#SCHEMA}.
   *
   * <p>The value type is stored as a string representation for compatibility with the schema
   * definition.
   *
   * @return A {@link Struct} representing the claim check.
   */
  public Struct toStruct() {
    return new Struct(ClaimCheckSchema.SCHEMA)
        .put(ClaimCheckSchemaFields.REFERENCE_URL, referenceUrl)
        .put(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES, originalSizeBytes)
        .put(ClaimCheckSchemaFields.UPLOADED_AT, uploadedAt)
        .put(ClaimCheckSchemaFields.ORIGINAL_SCHEMAS_ENABLED, originalSchemasEnabled)
        .put(ClaimCheckSchemaFields.ORIGINAL_SCHEMA_JSON, originalSchemaJson)
        .put(ClaimCheckSchemaFields.ORIGINAL_VALUE_TYPE, originalRecordValueType.type());
  }

  /**
   * Gets the original value type.
   *
   * @return The original value type.
   */
  public RecordValueType getOriginalValueType() {
    return originalRecordValueType;
  }
}
