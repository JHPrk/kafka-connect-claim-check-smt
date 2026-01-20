package com.github.cokelee777.kafka.connect.smt.claimcheck.internal;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonRecordSerializer implements RecordSerializer {

  private static final Logger log = LoggerFactory.getLogger(JsonRecordSerializer.class);

  private final JsonConverter schemaValueConverter;
  private final JsonConverter schemalessValueConverter;

  public JsonRecordSerializer(
      JsonConverter schemaValueConverter, JsonConverter schemalessValueConverter) {
    this.schemaValueConverter =
        Objects.requireNonNull(schemaValueConverter, "schemaValueConverter must not be null");
    this.schemalessValueConverter =
        Objects.requireNonNull(
            schemalessValueConverter, "schemalessValueConverter must not be null");
  }

  @Override
  public byte[] serialize(SourceRecord record) {
    String topic = record.topic();
    Schema schema = record.valueSchema();
    Object value = record.value();

    if (value == null) {
      return null;
    }

    if (schema != null) {
      try {
        return schemaValueConverter.fromConnectData(topic, schema, value);
      } catch (Exception e) {
        throw new SerializationException("Failed to serialize value with schema", e);
      }
    }

    if (value instanceof byte[]) {
      return (byte[]) value;
    }

    if (value instanceof String) {
      return ((String) value).getBytes(StandardCharsets.UTF_8);
    }

    if (value instanceof Map) {
      try {
        return schemalessValueConverter.fromConnectData(topic, null, value);
      } catch (Exception e) {
        throw new SerializationException("Failed to serialize value without schema", e);
      }
    }

    log.warn("Schemaless value of unsupported type: {}", value.getClass());
    return null;
  }
}
