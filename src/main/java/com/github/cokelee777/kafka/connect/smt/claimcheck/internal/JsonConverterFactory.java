package com.github.cokelee777.kafka.connect.smt.claimcheck.internal;

import java.util.Map;
import org.apache.kafka.connect.json.JsonConverter;

/**
 * A factory for creating pre-configured {@link JsonConverter} instances.
 *
 * <p>This class provides a convenient way to get a {@code JsonConverter} set up for serializing a
 * record's value without including schema information in the output.
 */
public class JsonConverterFactory {

  public static JsonConverter createSchemaValueConverter() {
    JsonConverter converter = new JsonConverter();
    Map<String, Object> configs = Map.of("schemas.enable", true, "converter.type", "value");
    converter.configure(configs);
    return converter;
  }

  public static JsonConverter createSchemalessValueConverter() {
    JsonConverter converter = new JsonConverter();
    Map<String, Object> configs = Map.of("schemas.enable", false, "converter.type", "value");
    converter.configure(configs);
    return converter;
  }

  private JsonConverterFactory() {}
}
