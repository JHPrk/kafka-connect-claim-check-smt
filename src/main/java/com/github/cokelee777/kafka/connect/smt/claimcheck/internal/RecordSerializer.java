package com.github.cokelee777.kafka.connect.smt.claimcheck.internal;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * An interface for serializing the value and schema of a {@link SourceRecord}.
 *
 * <p>This is used to determine the size of the record's payload before deciding whether to apply
 * the claim check pattern, and to preserve schema information for later restoration.
 */
public interface RecordSerializer {

  /**
   * Serializes the value of a source record into a byte array.
   *
   * @param record The source record to serialize.
   * @return A byte array representing the serialized record value, or {@code null} if the value
   *     cannot be serialized.
   */
  byte[] serializeValue(SourceRecord record);

  /**
   * Serializes the schema of a source record to a JSON string representation.
   *
   * <p>This is used to store schema metadata in claim check references for later restoration.
   *
   * @param schema The schema to serialize. Can be {@code null}.
   * @return A JSON string representation of the schema, or {@code null} if the input schema is
   *     {@code null}.
   */
  String serializeSchema(Schema schema);
}
