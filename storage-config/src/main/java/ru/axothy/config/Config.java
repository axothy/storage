package ru.axothy.config;

import java.nio.file.Path;

public record Config(
        Path basePath,
        long flushThresholdBytes,
        double bloomFilterFPP) {
}