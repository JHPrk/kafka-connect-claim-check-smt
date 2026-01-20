package com.github.cokelee777.kafka.connect.smt.claimcheck.model;

import java.util.Objects;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;

/**
 * Represents metadata about a source record that is needed for claim check restoration.
 *
 * <p>This immutable value object encapsulates the original value type, schema information, and
 * serialized schema JSON for later restoration in a sink transform.
 */
public final class RecordMetadata {

  private final boolean schemasEnabled;
  private final String schemaJson;
  private final ValueType valueType;

  private RecordMetadata(boolean schemasEnabled, String schemaJson, ValueType valueType) {
    this.schemasEnabled = schemasEnabled;
    this.schemaJson = schemaJson;
    this.valueType = Objects.requireNonNull(valueType, "valueType must not be null");
  }

  /**
   * Creates a new {@link RecordMetadata} instance.
   *
   * @param valueType The type of the value.
   * @param schemasEnabled Whether the record uses schemas.
   * @param schemaJson The serialized schema JSON, or {@code null} if schemas are not enabled.
   * @return A new {@link RecordMetadata} instance.
   */
  public static RecordMetadata create(
      boolean schemasEnabled, String schemaJson, ValueType valueType) {
    Objects.requireNonNull(valueType, "valueType must not be null");
    return new RecordMetadata(schemasEnabled, schemaJson, valueType);
  }

  /**
   * Creates a {@link RecordMetadata} from a schema.
   *
   * @param valueType The type of the value.
   * @param schema The schema, or {@code null} if schemas are not enabled.
   * @param schemaSerializer A function to serialize the schema to JSON.
   * @return A new {@link RecordMetadata} instance.
   */
  public static RecordMetadata fromSchema(
      Schema schema, Function<Schema, String> schemaSerializer, ValueType valueType) {
    boolean schemasEnabled = schema != null;
    String schemaJson = schemasEnabled ? schemaSerializer.apply(schema) : null;
    return create(schemasEnabled, schemaJson, valueType);
  }

  public ValueType getValueType() {
    return valueType;
  }

  public boolean isSchemasEnabled() {
    return schemasEnabled;
  }

  public String getSchemaJson() {
    return schemaJson;
  }
}
