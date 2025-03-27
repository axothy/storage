package ru.axothy.storage;

/**
 * Binary search in SSTable result information.
 */
public record FindResult(boolean found, long index) { }
