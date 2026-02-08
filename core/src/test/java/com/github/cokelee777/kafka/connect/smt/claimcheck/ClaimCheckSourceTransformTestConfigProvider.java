package com.github.cokelee777.kafka.connect.smt.claimcheck;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.ClaimCheckSourceTransformConfig;
import java.util.HashMap;
import java.util.Map;

public class ClaimCheckSourceTransformTestConfigProvider {

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final Map<String, String> configs = new HashMap<>();

    public Builder storageType(String storageType) {
      configs.put(ClaimCheckSourceTransformConfig.STORAGE_TYPE_CONFIG, storageType);
      return this;
    }

    public Builder thresholdBytes(int thresholdBytes) {
      configs.put(
          ClaimCheckSourceTransformConfig.THRESHOLD_BYTES_CONFIG, String.valueOf(thresholdBytes));
      return this;
    }

    public Map<String, String> build() {
      return new HashMap<>(configs);
    }
  }
}
