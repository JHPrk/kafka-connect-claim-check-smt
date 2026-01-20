package com.github.cokelee777.kafka.connect.smt.claimcheck.internal;

import com.github.cokelee777.kafka.connect.smt.claimcheck.model.RecordMetadata;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.RecordValueType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Extracts metadata from a {@link SourceRecord} for claim check restoration.
 *
 * <p>This class is responsible for determining the value type and extracting schema information
 * from source records.
 */
public class RecordMetadataExtractor {

  private final RecordSerializer recordSerializer;

  /**
   * Creates a new {@link RecordMetadataExtractor} with the provided serializer.
   *
   * @param recordSerializer The serializer to use for schema serialization.
   */
  public RecordMetadataExtractor(RecordSerializer recordSerializer) {
    this.recordSerializer = recordSerializer;
  }

  /**
   * Extracts metadata from a source record.
   *
   * @param record The source record to extract metadata from.
   * @return A {@link RecordMetadata} instance containing the extracted information.
   */
  public RecordMetadata extract(SourceRecord record) {
    Schema valueSchema = record.valueSchema();
    boolean schemasEnabled = valueSchema != null;
    String schemaJson = schemasEnabled ? recordSerializer.serializeSchema(valueSchema) : null;
    RecordValueType recordValueType = RecordValueType.from(record);

    return RecordMetadata.create(schemasEnabled, schemaJson, recordValueType);
  }
}
