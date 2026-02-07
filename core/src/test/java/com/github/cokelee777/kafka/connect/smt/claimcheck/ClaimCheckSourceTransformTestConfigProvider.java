package com.github.cokelee777.kafka.connect.smt.claimcheck;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.ClaimCheckSourceTransformConfig;
import java.util.HashMap;
import java.util.Map;

public class ClaimCheckSourceTransformTestConfigProvider {

  public static Map<String, String> config() {
    return new HashMap<>();
  }

  public static Map<String, String> config(String storageType) {
    Map<String, String> configs = config();
    configs.put(ClaimCheckSourceTransformConfig.STORAGE_TYPE_CONFIG, storageType);
    return configs;
  }

  public static Map<String, String> config(String storageType, int thresholdBytes) {
    Map<String, String> configs = config(storageType);
    configs.put(
        ClaimCheckSourceTransformConfig.THRESHOLD_BYTES_CONFIG, String.valueOf(thresholdBytes));
    return configs;
  }
}
