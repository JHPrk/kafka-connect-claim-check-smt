package com.github.cokelee777.kafka.connect.smt.claimcheck.model;

import java.util.Objects;

/**
 * Represents metadata about a source record that is needed for claim check restoration.
 *
 * <p>This immutable value object encapsulates the original record value type, schema information,
 * and serialized schema JSON for later restoration in a sink transform.
 */
public final class RecordMetadata {

  private final boolean schemasEnabled;
  private final String schemaJson;
  private final RecordValueType recordValueType;

  private RecordMetadata(
      boolean schemasEnabled, String schemaJson, RecordValueType recordValueType) {
    this.schemasEnabled = schemasEnabled;
    this.schemaJson = schemaJson;
    this.recordValueType =
        Objects.requireNonNull(recordValueType, "recordValueType must not be null");
  }

  /**
   * Creates a new {@link RecordMetadata} instance.
   *
   * @param recordValueType The type of the value.
   * @param schemasEnabled Whether the record uses schemas.
   * @param schemaJson The serialized schema JSON, or {@code null} if schemas are not enabled.
   * @return A new {@link RecordMetadata} instance.
   */
  public static RecordMetadata create(
      boolean schemasEnabled, String schemaJson, RecordValueType recordValueType) {
    Objects.requireNonNull(recordValueType, "recordValueType must not be null");
    return new RecordMetadata(schemasEnabled, schemaJson, recordValueType);
  }

  public RecordValueType getRecordValueType() {
    return recordValueType;
  }

  public boolean isSchemasEnabled() {
    return schemasEnabled;
  }

  public String getSchemaJson() {
    return schemaJson;
  }
}
