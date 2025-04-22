package utils;

import ru.axothy.api.Entry;
import ru.axothy.api.Storage;
import ru.axothy.config.Config;
import ru.axothy.storage.LSMStorage;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

public class MemorySegmentStorageFactory implements StorageFactory<MemorySegment, Entry<MemorySegment>> {

    @Override
    public Storage<MemorySegment, Entry<MemorySegment>> createStorage(Config config) {
        return new LSMStorage(config);
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        return memorySegment == null ? null :
                new String(memorySegment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    @Override
    public MemorySegment fromString(String data) {
        return data == null ? null : MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}