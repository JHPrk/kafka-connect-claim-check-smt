package com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.type;

import static org.assertj.core.api.Assertions.*;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SchemalessRecordValuePlaceholderTest {

  private SchemalessRecordValuePlaceholder schemalessPlaceholderStrategy;

  @BeforeEach
  void setUp() {
    schemalessPlaceholderStrategy = new SchemalessRecordValuePlaceholder();
  }

  @Nested
  class ApplyTest {

    @Test
    void shouldReturnNullForSchemalessRecord() {
      // Given
      String value = "payload";
      SourceRecord record =
          new SourceRecord(null, null, "test-topic", Schema.BYTES_SCHEMA, "key", null, value);

      // When
      Object defaultValue = schemalessPlaceholderStrategy.apply(record);

      // Then
      assertThat(defaultValue).isNull();
    }

    @Test
    void shouldThrowExceptionWhenRecordHasSchema() {
      // Given
      String value = "payload";
      SourceRecord record =
          new SourceRecord(
              null, null, "test-topic", Schema.BYTES_SCHEMA, "key", Schema.STRING_SCHEMA, value);

      // When & Then
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> schemalessPlaceholderStrategy.apply(record))
          .withMessage("Cannot handle record with non-null schema. Expected schemaless record.");
    }
  }
}
