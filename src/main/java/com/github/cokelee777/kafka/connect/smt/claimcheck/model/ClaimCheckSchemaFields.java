package com.github.cokelee777.kafka.connect.smt.claimcheck.model;

/**
 * Contains constants for the field names used in the {@link ClaimCheckSchema}.
 *
 * <p>This prevents magic strings and provides a single source of truth for field names.
 */
public final class ClaimCheckSchemaFields {

  private ClaimCheckSchemaFields() {}

  /** The field name for the URL pointing to the externally stored payload. */
  public static final String REFERENCE_URL = "reference_url";

  /** The field name for the original size of the payload in bytes. */
  public static final String ORIGINAL_SIZE_BYTES = "original_size_bytes";

  /** The field name for the Unix timestamp (in milliseconds) when the payload was uploaded. */
  public static final String UPLOADED_AT = "uploaded_at";

  /** The field name indicating whether the original record used schemas. */
  public static final String ORIGINAL_SCHEMAS_ENABLED = "original_schemas_enabled";

  /** The field name for the serialized schema JSON, or {@code null} if schemas were not enabled. */
  public static final String ORIGINAL_SCHEMA_JSON = "original_schema_json";

  /** The field name for the type of the original value. */
  public static final String ORIGINAL_VALUE_TYPE = "original_value_type";
}
