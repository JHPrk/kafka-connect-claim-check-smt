package com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder;

/** Supported placeholder strategy types. */
public enum RecordValuePlaceholderType {
  SCHEMALESS("schemaless"),
  DEBEZIUM_STRUCT("debezium_struct"),
  GENERIC_STRUCT("generic_struct");

  private final String type;

  RecordValuePlaceholderType(String type) {
    this.type = type;
  }

  /** Returns the type identifier string. */
  public String type() {
    return type;
  }
}
