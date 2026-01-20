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

/**
 * A {@link RecordSerializer} implementation that serializes record values to JSON byte arrays and
 * handles schema serialization.
 *
 * <p>This implementation handles various value types:
 *
 * <ul>
 *   <li>{@code byte[]}: Passed through directly.
 *   <li>{@code String}: Converted to UTF-8 bytes.
 *   <li>Schema with Struct: Serialized to JSON using schema-enabled converter.
 *   <li>Schemaless Map: Serialized to JSON using schemaless converter.
 * </ul>
 *
 * <p>This class also provides schema serialization capabilities for storing schema metadata in
 * claim check references.
 */
public class JsonRecordSerializer implements RecordSerializer {

  private static final Logger log = LoggerFactory.getLogger(JsonRecordSerializer.class);

  private final JsonConverter schemaValueConverter;
  private final JsonConverter schemalessValueConverter;

  /**
   * Constructs a serializer with the provided converters.
   *
   * @param schemaValueConverter The converter to use for records with schema (schemas.enable=true).
   * @param schemalessValueConverter The converter to use for schemaless records
   *     (schemas.enable=false).
   */
  public JsonRecordSerializer(
      JsonConverter schemaValueConverter, JsonConverter schemalessValueConverter) {
    this.schemaValueConverter =
        Objects.requireNonNull(schemaValueConverter, "schemaValueConverter must not be null");
    this.schemalessValueConverter =
        Objects.requireNonNull(
            schemalessValueConverter, "schemalessValueConverter must not be null");
  }

  /**
   * Serializes the record's value.
   *
   * @param record The record whose value needs to be serialized.
   * @return The serialized value as a byte array.
   * @throws SerializationException if the JSON conversion fails.
   */
  @Override
  public byte[] serializeValue(SourceRecord record) {
    String topic = record.topic();
    Object value = record.value();
    Schema valueSchema = record.valueSchema();

    if (value == null) {
      return null;
    }

    if (valueSchema != null) {
      try {
        return schemaValueConverter.fromConnectData(topic, valueSchema, value);
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

  /**
   * Serializes a {@link Schema} to a JSON string representation.
   *
   * <p>This method uses {@link Schema#toString()} to serialize the schema to JSON Schema format.
   * While values are serialized using JsonConverter, Schema objects have a built-in toString()
   * method that produces a JSON-compatible representation of the schema structure, which is the
   * standard way to serialize Kafka Connect schemas.
   *
   * <p>Note: JsonConverter.fromConnectData() requires both schema and value, so it cannot be used
   * to serialize a schema alone. Schema.toString() is the appropriate method for schema-only
   * serialization and produces a JSON Schema-compatible format.
   *
   * @param schema The schema to serialize. Can be {@code null}.
   * @return A JSON string representation of the schema, or {@code null} if the input schema is
   *     {@code null}.
   * @throws SerializationException if the schema cannot be serialized.
   */
  @Override
  public String serializeSchema(Schema schema) {
    if (schema == null) {
      return null;
    }

    try {
      // Schema.toString() is the standard way to serialize Kafka Connect Schema to JSON Schema
      // format
      // Unlike values which use JsonConverter, schemas have a built-in toString() that produces
      // JSON-compatible output, which is the appropriate serialization method for schema-only data
      return schema.toString();
    } catch (Exception e) {
      throw new SerializationException("Failed to serialize schema", e);
    }
  }
}
