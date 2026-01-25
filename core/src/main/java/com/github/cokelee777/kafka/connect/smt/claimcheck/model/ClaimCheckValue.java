package com.github.cokelee777.kafka.connect.smt.claimcheck.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.connect.data.Struct;

public class ClaimCheckValue {

  private final String referenceUrl;
  private final long originalSizeBytes;
  private final long uploadedAt;

  private ClaimCheckValue(String referenceUrl, long originalSizeBytes, long uploadedAt) {
    this.referenceUrl = referenceUrl;
    this.originalSizeBytes = originalSizeBytes;
    this.uploadedAt = uploadedAt;
  }

  public String getReferenceUrl() {
    return referenceUrl;
  }

  public long getOriginalSizeBytes() {
    return originalSizeBytes;
  }

  public static ClaimCheckValue create(String referenceUrl, long originalSizeBytes) {
    if (referenceUrl == null || referenceUrl.isBlank()) {
      throw new IllegalArgumentException("referenceUrl must be non-blank");
    }

    if (originalSizeBytes < 0) {
      throw new IllegalArgumentException("originalSizeBytes must be >= 0");
    }

    return new ClaimCheckValue(referenceUrl, originalSizeBytes, Instant.now().toEpochMilli());
  }

  public Struct toStruct() {
    return new Struct(ClaimCheckSchema.SCHEMA)
        .put(ClaimCheckSchemaFields.REFERENCE_URL, referenceUrl)
        .put(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES, originalSizeBytes)
        .put(ClaimCheckSchemaFields.UPLOADED_AT, uploadedAt);
  }

  public static ClaimCheckValue from(Struct struct) {
    return new ClaimCheckValue(
        struct.getString(ClaimCheckSchemaFields.REFERENCE_URL),
        struct.getInt64(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES),
        struct.getInt64(ClaimCheckSchemaFields.UPLOADED_AT));
  }

  public static ClaimCheckValue from(JsonNode node) {
    return new ClaimCheckValue(
        node.get(ClaimCheckSchemaFields.REFERENCE_URL).asText(),
        node.get(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES).asLong(),
        node.get(ClaimCheckSchemaFields.UPLOADED_AT).asLong());
  }

  public static ClaimCheckValue from(Map<?, ?> map) {
    return new ClaimCheckValue(
        map.get(ClaimCheckSchemaFields.REFERENCE_URL).toString(),
        Long.parseLong(map.get(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES).toString()),
        Long.parseLong(map.get(ClaimCheckSchemaFields.UPLOADED_AT).toString()));
  }
}
