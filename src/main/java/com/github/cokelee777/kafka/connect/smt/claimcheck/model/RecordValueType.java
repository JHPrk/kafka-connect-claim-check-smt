package com.github.cokelee777.kafka.connect.smt.claimcheck.model;

import java.util.Map;
import java.util.Objects;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Enumerates the possible value types for Kafka Connect source records.
 *
 * <p>This enum represents the different types of values that can be present in a {@link
 * SourceRecord}, which is used to determine how to serialize and deserialize the value for claim
 * check storage and restoration.
 */
public enum RecordValueType {
  /**
   * A structured value with a schema (typically a {@link org.apache.kafka.connect.data.Struct}).
   */
  STRUCT("struct"),

  /** A raw byte array value. */
  BYTES("bytes"),

  /** A string value. */
  STRING("string"),

  /** A schemaless map value (typically {@link Map}). */
  MAP("map"),

  /** A null value. */
  NULL("null"),

  /** An unknown or unsupported value type (fallback). */
  UNKNOWN("unknown");

  private final String type;

  RecordValueType(String type) {
    this.type = type;
  }

  /**
   * Returns the string identifier for the value type.
   *
   * <p>This is used when serializing claim check references to store the value type as a string.
   *
   * @return The lower-case type string (e.g., "struct", "bytes").
   */
  public String type() {
    return type;
  }

  /**
   * Determines the value type from a source record.
   *
   * @param record The source record to analyze.
   * @return The corresponding {@link RecordValueType}.
   */
  public static RecordValueType from(SourceRecord record) {
    Objects.requireNonNull(record, "record must not be null");

    Schema valueSchema = record.valueSchema();
    Object value = record.value();

    if (value == null) {
      return NULL;
    }

    if (valueSchema != null) {
      return STRUCT;
    }

    if (value instanceof byte[]) {
      return BYTES;
    }

    if (value instanceof String) {
      return STRING;
    }

    if (value instanceof Map) {
      return MAP;
    }

    return UNKNOWN;
  }
}
