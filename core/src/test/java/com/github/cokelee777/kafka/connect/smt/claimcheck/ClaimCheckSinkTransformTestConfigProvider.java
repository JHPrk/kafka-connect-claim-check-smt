package com.github.cokelee777.kafka.connect.smt.claimcheck;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.ClaimCheckSinkTransformConfig;
import java.util.HashMap;
import java.util.Map;

public class ClaimCheckSinkTransformTestConfigProvider {

  public static Map<String, String> config() {
    return new HashMap<>();
  }

  public static Map<String, String> config(String storageType) {
    Map<String, String> configs = config();
    configs.put(ClaimCheckSinkTransformConfig.STORAGE_TYPE_CONFIG, storageType);
    return configs;
  }
}
