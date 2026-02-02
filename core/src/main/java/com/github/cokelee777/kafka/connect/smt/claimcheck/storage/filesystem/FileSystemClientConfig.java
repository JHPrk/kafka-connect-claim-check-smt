package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.filesystem;

public record FileSystemClientConfig(int retryMax, long retryBackoffMs, long retryMaxBackoffMs) {}
