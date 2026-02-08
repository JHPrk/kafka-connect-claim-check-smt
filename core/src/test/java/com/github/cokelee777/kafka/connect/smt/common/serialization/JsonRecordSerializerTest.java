package com.github.cokelee777.kafka.connect.smt.common.serialization;

import static org.assertj.core.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class JsonRecordSerializerTest {

  @Nested
  class CreateTest {

    @Test
    void shouldCreateWithSchemaAndSchemalessConverters() {
      // Given & When
      JsonRecordSerializer jsonRecordSerializer = JsonRecordSerializer.create();

      // Then
      assertThat(jsonRecordSerializer).isNotNull();
      assertThat(jsonRecordSerializer.getSchemaValueConverter()).isNotNull();
      assertThat(jsonRecordSerializer.getSchemalessValueConverter()).isNotNull();
    }
  }

  @Nested
  class SerializeTest {

    private JsonRecordSerializer jsonRecordSerializer;

    @BeforeEach
    void setUp() {
      jsonRecordSerializer = JsonRecordSerializer.create();
    }

    @Test
    void shouldSerializeRecordWithSchema() {
      // Given
      SourceRecord record =
          new SourceRecord(null, null, "test-bucket", Schema.STRING_SCHEMA, "payload");
      String expectedJson =
          "{\"schema\":{\"type\":\"string\",\"optional\":false},\"payload\":\"payload\"}";
      byte[] expectedPayload = expectedJson.getBytes(StandardCharsets.UTF_8);

      // When
      byte[] serializedPayload = jsonRecordSerializer.serialize(record);

      // Then
      assertThat(serializedPayload).isEqualTo(expectedPayload);
    }

    @Test
    void shouldSerializeSchemalessRecordWithBytes() {
      // Given
      SourceRecord record =
          new SourceRecord(
              null, null, "test-bucket", null, "payload".getBytes(StandardCharsets.UTF_8));
      byte[] expectedPayload = "payload".getBytes(StandardCharsets.UTF_8);

      // When
      byte[] serializedPayload = jsonRecordSerializer.serialize(record);

      // Then
      assertThat(serializedPayload).isEqualTo(expectedPayload);
    }

    @Test
    void shouldSerializeSchemalessRecordWithString() {
      // Given
      SourceRecord record = new SourceRecord(null, null, "test-bucket", null, "payload");
      byte[] expectedPayload = "payload".getBytes(StandardCharsets.UTF_8);

      // When
      byte[] serializedPayload = jsonRecordSerializer.serialize(record);

      // Then
      assertThat(serializedPayload).isEqualTo(expectedPayload);
    }

    @Test
    void shouldSerializeSchemalessRecordWithMap() {
      // Given
      Map<String, Object> payload = new LinkedHashMap<>();
      payload.put("id", 1);
      payload.put("name", "payload");
      SourceRecord record = new SourceRecord(null, null, "test-bucket", null, payload);
      String expectedJson = "{\"id\":1,\"name\":\"payload\"}";
      byte[] expectedPayload = expectedJson.getBytes(StandardCharsets.UTF_8);

      // When
      byte[] serializedPayload = jsonRecordSerializer.serialize(record);

      // Then
      assertThat(serializedPayload).isEqualTo(expectedPayload);
    }

    @Test
    void shouldReturnNullWhenRecordValueIsNull() {
      // Given
      SourceRecord record = new SourceRecord(null, null, "test-bucket", null, null);

      // When
      byte[] serializedPayload = jsonRecordSerializer.serialize(record);

      // Then
      assertThat(serializedPayload).isNull();
    }
  }
}
