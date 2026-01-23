package com.github.cokelee777.kafka.connect.smt.claimcheck.source;

import com.github.cokelee777.kafka.connect.smt.claimcheck.internal.RecordSerializer;
import com.github.cokelee777.kafka.connect.smt.claimcheck.internal.RecordSerializerFactory;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckReference;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchema;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageFactory;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.StorageType;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClaimCheckSourceTransform implements Transformation<SourceRecord> {

  private static final Logger log = LoggerFactory.getLogger(ClaimCheckSourceTransform.class);

  public static final class Config {

    public static final String STORAGE_TYPE = "storage.type";
    public static final String THRESHOLD_BYTES = "threshold.bytes";

    /** Default threshold: 1MB (1024 * 1024 bytes) */
    private static final long DEFAULT_THRESHOLD_BYTES = 1024L * 1024L;

    public static final ConfigDef DEFINITION =
        new ConfigDef()
            .define(
                STORAGE_TYPE,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.ValidString.in(StorageType.S3.type()),
                ConfigDef.Importance.HIGH,
                "Storage implementation type")
            .define(
                THRESHOLD_BYTES,
                ConfigDef.Type.LONG,
                DEFAULT_THRESHOLD_BYTES,
                ConfigDef.Range.atLeast(1L),
                ConfigDef.Importance.HIGH,
                "Payload size threshold in bytes");

    private Config() {}
  }

  private static class TransformConfig extends AbstractConfig {
    TransformConfig(Map<String, ?> originals) {
      super(Config.DEFINITION, originals);
    }
  }

  private String storageType;
  private long thresholdBytes;
  private ClaimCheckStorage storage;
  private RecordSerializer recordSerializer;

  public ClaimCheckSourceTransform() {}

  ClaimCheckSourceTransform(ClaimCheckStorage storage) {
    this.storage = storage;
  }

  ClaimCheckSourceTransform(RecordSerializer recordSerializer) {
    this.recordSerializer = recordSerializer;
  }

  ClaimCheckSourceTransform(ClaimCheckStorage storage, RecordSerializer recordSerializer) {
    this.storage = storage;
    this.recordSerializer = recordSerializer;
  }

  public ClaimCheckStorage getStorage() {
    return this.storage;
  }

  public String getStorageType() {
    return storageType;
  }

  public RecordSerializer getRecordSerializer() {
    return recordSerializer;
  }

  public long getThresholdBytes() {
    return this.thresholdBytes;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    TransformConfig config = new TransformConfig(configs);

    this.thresholdBytes = config.getLong(Config.THRESHOLD_BYTES);
    this.storageType = config.getString(Config.STORAGE_TYPE);

    if (this.storage == null) {
      this.storage = ClaimCheckStorageFactory.create(this.storageType);
    }
    Objects.requireNonNull(this.storage, "ClaimCheckStorage not configured");
    this.storage.configure(configs);

    if (this.recordSerializer == null) {
      this.recordSerializer = RecordSerializerFactory.create();
    }
    Objects.requireNonNull(this.recordSerializer, "RecordSerializer not configured");
  }

  @Override
  public SourceRecord apply(SourceRecord record) {
    if (record.value() == null) {
      return record;
    }

    byte[] serializedRecord = this.recordSerializer.serialize(record);
    if (serializedRecord == null || serializedRecord.length <= this.thresholdBytes) {
      return record;
    }

    return createClaimCheckRecord(record, serializedRecord);
  }

  private SourceRecord createClaimCheckRecord(SourceRecord record, byte[] serializedRecord) {
    String referenceUrl = this.storage.store(serializedRecord);
    Struct referenceValue =
        ClaimCheckReference.create(referenceUrl, serializedRecord.length).toStruct();

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        ClaimCheckSchema.SCHEMA,
        referenceValue,
        record.timestamp());
  }

  @Override
  public ConfigDef config() {
    return Config.DEFINITION;
  }

  @Override
  public void close() {
    if (this.storage != null) {
      this.storage.close();
    }
  }
}
