package com.github.cokelee777.kafka.connect.smt.common.utils;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PathUtilsTest {

  @Nested
  class NormalizePathPrefixTest {

    @Test
    void shouldReturnUnchangedWhenPathIsAlreadyNormalized() {
      // Given
      String originalPathPrefix = "dir1/dir2";

      // When
      String normalizedPathPrefix = PathUtils.normalizePathPrefix(originalPathPrefix);

      // Then
      assertThat(normalizedPathPrefix).isEqualTo(originalPathPrefix);
    }

    @Test
    void shouldRemoveTrailingSlash() {
      // Given
      String originalPathPrefix = "dir1/dir2/";

      // When
      String normalizedPathPrefix = PathUtils.normalizePathPrefix(originalPathPrefix);

      // Then
      assertThat(normalizedPathPrefix).isNotEqualTo(originalPathPrefix);
      assertThat(normalizedPathPrefix).doesNotEndWith("/");
      assertThat(normalizedPathPrefix)
          .isEqualTo(originalPathPrefix.substring(0, originalPathPrefix.length() - 1));
    }

    @Test
    void shouldRemoveLeadingSlash() {
      // Given
      String originalPathPrefix = "/dir1/dir2";

      // When
      String normalizedPathPrefix = PathUtils.normalizePathPrefix(originalPathPrefix);

      // Then
      assertThat(normalizedPathPrefix).isNotEqualTo(originalPathPrefix);
      assertThat(normalizedPathPrefix).doesNotStartWith("/");
      assertThat(normalizedPathPrefix).isEqualTo(originalPathPrefix.substring(1));
    }

    @Test
    void shouldRemoveMultipleSlashesAtBothEnds() {
      // Given
      String originalPathPrefix = "//dir1/dir2//";

      // When
      String normalizedPathPrefix = PathUtils.normalizePathPrefix(originalPathPrefix);

      // Then
      assertThat(normalizedPathPrefix).isNotEqualTo(originalPathPrefix);
      assertThat(normalizedPathPrefix).doesNotStartWith("/");
      assertThat(normalizedPathPrefix).doesNotEndWith("/");
      assertThat(normalizedPathPrefix)
          .isEqualTo(originalPathPrefix.substring(2, originalPathPrefix.length() - 2));
    }

    @Test
    void shouldCollapseConsecutiveSlashesInMiddle() {
      // Given
      String originalPathPrefix = "dir1//dir2";

      // When
      String normalizePathPrefix = PathUtils.normalizePathPrefix(originalPathPrefix);

      // Then
      assertThat(normalizePathPrefix).isNotEqualTo(originalPathPrefix);
      assertThat(normalizePathPrefix).doesNotContain("//");
      assertThat(normalizePathPrefix.length()).isEqualTo(originalPathPrefix.length() - 1);
      assertThat(normalizePathPrefix)
          .isEqualTo(originalPathPrefix.substring(0, 5) + originalPathPrefix.substring(6));
    }
  }
}
