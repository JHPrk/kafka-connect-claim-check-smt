package com.github.cokelee777.kafka.connect.smt.claimcheck.config;

import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.ClaimCheckSourceTransformConfig.THRESHOLD_BYTES_CONFIG;
import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.ClaimCheckSourceTransformConfig.THRESHOLD_BYTES_DEFAULT;
import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.ClaimCheckTransformConfig.STORAGE_TYPE_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.github.cokelee777.kafka.connect.smt.claimcheck.ClaimCheckSourceTransformTestConfigProvider;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ClaimCheckSourceTransformConfigTest {

  @Nested
  class ConstructorTest {

    @Test
    void shouldConstructWithAllProvidedArguments() {
      // Given
      Map<String, String> configs =
          ClaimCheckSourceTransformTestConfigProvider.config(ClaimCheckStorageType.S3.type(), 1024);

      // When
      ClaimCheckSourceTransformConfig config = new ClaimCheckSourceTransformConfig(configs);

      // Then
      assertThat(config.getStorageType()).isEqualTo(ClaimCheckStorageType.S3.type());
      assertThat(config.getThresholdBytes()).isEqualTo(1024);
    }

    @Test
    void shouldUseDefaultValuesWhenOptionalArgumentsNotProvided() {
      // Given
      Map<String, String> configs =
          ClaimCheckSourceTransformTestConfigProvider.config(ClaimCheckStorageType.S3.type());

      // When
      ClaimCheckSourceTransformConfig config = new ClaimCheckSourceTransformConfig(configs);

      // Then
      assertThat(config.getStorageType()).isEqualTo(ClaimCheckStorageType.S3.type());
      assertThat(config.getThresholdBytes()).isEqualTo(THRESHOLD_BYTES_DEFAULT);
    }

    @Test
    void shouldThrowConfigExceptionWhenStorageTypeIsMissing() {
      // Given
      Map<String, String> configs = ClaimCheckSourceTransformTestConfigProvider.config();

      // When & Then
      assertThatExceptionOfType(ConfigException.class)
          .isThrownBy(() -> new ClaimCheckSourceTransformConfig(configs))
          .withMessage(
              "Missing required configuration \""
                  + STORAGE_TYPE_CONFIG
                  + "\" which has no default value.");
    }
  }

  @Nested
  class ConfigDefValidationTest {

    @Test
    void shouldReturnNonNullConfigDef() {
      // When
      ConfigDef configDef = ClaimCheckSourceTransformConfig.configDef();

      // Then
      assertThat(configDef).isNotNull();
      assertThat(configDef.names()).contains(STORAGE_TYPE_CONFIG, THRESHOLD_BYTES_CONFIG);
    }

    @Test
    void shouldValidateStorageTypeConfigAcceptsValidValues() {
      // Given
      Map<String, String> validConfigs =
          ClaimCheckSourceTransformTestConfigProvider.config(ClaimCheckStorageType.S3.type(), 1024);

      // When & Then
      ClaimCheckSinkTransformConfig.configDef().parse(validConfigs);
    }

    @Test
    void shouldValidateStorageTypeConfigRejectsInvalidValues() {
      // Given
      Map<String, String> invalidConfigs = Collections.singletonMap(STORAGE_TYPE_CONFIG, "INVALID");

      // When & Then
      assertThatExceptionOfType(ConfigException.class)
          .isThrownBy(() -> ClaimCheckSinkTransformConfig.configDef().parse(invalidConfigs))
          .withMessageContaining("Invalid value INVALID for configuration " + STORAGE_TYPE_CONFIG);
    }
  }
}
