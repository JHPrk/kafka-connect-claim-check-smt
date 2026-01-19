package com.github.cokelee777.kafka.connect.smt.claimcheck.internal;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RecordValueSerializer} that serializes the record's value to a JSON byte array.
 *
 * <p>This implementation handles various value types:
 *
 * <ul>
 *   <li>{@code byte[]}: Passed through directly.
 *   <li>{@code String}: Converted to UTF-8 bytes.
 *   <li>Other types: Serialized to JSON using the provided {@link JsonConverter}.
 * </ul>
 */
public class JsonRecordValueSerializer implements RecordValueSerializer {

  private static final Logger log = LoggerFactory.getLogger(JsonRecordValueSerializer.class);

  private final JsonConverter jsonConverter;

  /**
   * Constructs a serializer with a specific {@link JsonConverter}.
   *
   * @param jsonConverter The converter to use for serialization.
   */
  public JsonRecordValueSerializer(JsonConverter jsonConverter) {
    this.jsonConverter = Objects.requireNonNull(jsonConverter, "jsonConverter must not be null");
  }

  /**
   * Serializes the record's value.
   *
   * @param record The record whose value needs to be serialized.
   * @return The serialized value as a byte array.
   * @throws SerializationException if the JSON conversion fails.
   */
  @Override
  public byte[] serialize(SourceRecord record) {
    Object value = record.value();
    Schema valueSchema = record.valueSchema();

    if (value == null) {
      return null;
    }

    if (valueSchema != null) {
      try {
        return jsonConverter.fromConnectData(record.topic(), valueSchema, value);
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

    log.warn("Schemaless value of unsupported type: {}", value.getClass());
    return null;
  }
}
