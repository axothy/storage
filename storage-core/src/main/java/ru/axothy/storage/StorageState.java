package ru.axothy.storage;

import ru.axothy.api.Entry;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public final class StorageState {
    private static final Comparator<MemorySegment> comparator = LSMStorage::comparator;
    private final SortedMap<MemorySegment, Entry<MemorySegment>> readEntries;
    private final SortedMap<MemorySegment, Entry<MemorySegment>> writeEntries;
    private final List<MemorySegment> sstables;

    private StorageState(SortedMap<MemorySegment, Entry<MemorySegment>> readEntries, SortedMap<MemorySegment, Entry<MemorySegment>> writeEntries, List<MemorySegment> segments) {
        this.readEntries = readEntries;
        this.writeEntries = writeEntries;
        this.sstables = segments;
    }

    private static SortedMap<MemorySegment, Entry<MemorySegment>> createMap() {
        return new ConcurrentSkipListMap<>(comparator);
    }

    public static StorageState initial(List<MemorySegment> segments) {
        return new StorageState(createMap(), createMap(), segments);
    }

    public StorageState compact(MemorySegment compacted) {
        return new StorageState(
                readEntries,
                writeEntries,
                compacted == null ? Collections.emptyList() : Collections.singletonList(compacted));
    }

    public StorageState beforeFlush() {
        return new StorageState(writeEntries, createMap(), sstables);
    }

    public StorageState afterFlush(MemorySegment newPage) {
        List<MemorySegment> segments = new ArrayList<>(this.sstables.size() + 1);
        segments.addAll(this.sstables);
        segments.add(newPage);

        return new StorageState(createMap(), writeEntries, segments);
    }

    public SortedMap<MemorySegment, Entry<MemorySegment>> getReadEntries() {
        return readEntries;
    }

    public SortedMap<MemorySegment, Entry<MemorySegment>> getWriteEntries() {
        return writeEntries;
    }

    public List<MemorySegment> getSstables() {
        return sstables;
    }
}
