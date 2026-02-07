package com.github.cokelee777.kafka.connect.smt.claimcheck.config;

import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.ClaimCheckTransformConfig.STORAGE_TYPE_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ClaimCheckTransformConfigTest {

  @Nested
  class NewConfigDefTest {

    @Test
    void shouldReturnConfigDefWithStorageTypeConfigDefined() {
      // When
      ConfigDef configDef = ClaimCheckTransformConfig.newConfigDef();

      // Then
      assertThat(configDef).isNotNull();
      assertThat(configDef.names()).contains(STORAGE_TYPE_CONFIG);
    }
  }
}
