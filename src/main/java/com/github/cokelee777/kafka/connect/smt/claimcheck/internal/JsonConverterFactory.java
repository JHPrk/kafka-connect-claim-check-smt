package com.github.cokelee777.kafka.connect.smt.claimcheck.internal;

import java.util.Map;
import org.apache.kafka.connect.json.JsonConverter;

public class JsonConverterFactory {

  public static JsonRecordSerializer create() {
    // Converter for records with schema.
    JsonConverter schemaValueConverter = new JsonConverter();
    schemaValueConverter.configure(Map.of("schemas.enable", true), false);

    // Converter for schemaless records.
    JsonConverter schemalessValueConverter = new JsonConverter();
    schemalessValueConverter.configure(Map.of("schemas.enable", false), false);

    return new JsonRecordSerializer(schemaValueConverter, schemalessValueConverter);
  }

  private JsonConverterFactory() {}
}
