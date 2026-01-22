package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.retry;

import static org.assertj.core.api.Assertions.*;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("RetryConfig 단위 테스트")
class RetryConfigTest {

  @Nested
  @DisplayName("생성자 테스트")
  class Constructor {

    @Test
    @DisplayName("올바른 인자를 세팅하면 정상적으로 객체가 생성된다.")
    public void normalArgs() {
      // Given
      int maxAttempts = 3;
      Duration initialBackoff = Duration.ofMillis(300L);
      Duration maxBackoff = Duration.ofMillis(20000L);

      // When
      RetryConfig retryConfig = new RetryConfig(maxAttempts, initialBackoff, maxBackoff);

      // Then
      assertThat(retryConfig).isNotNull();
      assertThat(retryConfig.maxAttempts()).isSameAs(maxAttempts);
      assertThat(retryConfig.initialBackoff()).isSameAs(initialBackoff);
      assertThat(retryConfig.maxBackoff()).isSameAs(maxBackoff);
    }

    @Test
    @DisplayName("maxAttempts를 0보다 낮게 설정하면 예외가 발생한다.")
    public void setMaxAttemptsLessThanZeroCauseException() {
      // Given
      int maxAttempts = -1;
      Duration initialBackoff = Duration.ofMillis(300L);
      Duration maxBackoff = Duration.ofMillis(20000L);

      // When & Then
      assertThatThrownBy(() -> new RetryConfig(maxAttempts, initialBackoff, maxBackoff))
          .isExactlyInstanceOf(IllegalArgumentException.class)
          .hasMessage("maxAttempts must be >= 0");
    }

    @Test
    @DisplayName("initialBackoff를 0보다 낮게 설정하면 예외가 발생한다.")
    public void setInitialBackoffLessThanZeroCauseException() {
      // Given
      int maxAttempts = 3;
      Duration initialBackoff = Duration.ofMillis(-300L);
      Duration maxBackoff = Duration.ofMillis(20000L);

      // When & Then
      assertThatThrownBy(() -> new RetryConfig(maxAttempts, initialBackoff, maxBackoff))
          .isExactlyInstanceOf(IllegalArgumentException.class)
          .hasMessage("initialBackoff must be > 0");
    }

    @Test
    @DisplayName("maxBackoff 0보다 낮게 설정하면 예외가 발생한다.")
    public void setMaxBackoffLessThanZeroCauseException() {
      // Given
      int maxAttempts = 3;
      Duration initialBackoff = Duration.ofMillis(300L);
      Duration maxBackoff = Duration.ofMillis(-20000L);

      // When & Then
      assertThatThrownBy(() -> new RetryConfig(maxAttempts, initialBackoff, maxBackoff))
          .isExactlyInstanceOf(IllegalArgumentException.class)
          .hasMessage("maxBackoff must be > 0");
    }
  }
}
