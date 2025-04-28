package ru.axothy.utils;

import com.google.protobuf.ByteString;
import lsmraft.*;
import ru.axothy.api.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class ProtoEntryConverters {

    private ProtoEntryConverters() {

    }

    public static Lsmraft.KvEntry toProtoEntry(Entry<MemorySegment> entry) {
        return Lsmraft.KvEntry.newBuilder()
                .setKey(toBytes(entry.key())) //fixme в один метод разместить это все можно
                .setValue(toBytes(entry.value()))
                .build();
    }

    public static ByteString toBytes(MemorySegment seg) {
        return ByteString.copyFrom(toByteArray(seg));
    }

    public static MemorySegment toSegment(ByteString bs) {
        return toSegment(bs.toByteArray());
    }

    public static MemorySegment toSegment(byte[] bytes) {
        return MemorySegment.ofArray(bytes);
    }

    public static byte[] toByteArray(MemorySegment memorySegment) {
        return memorySegment.toArray(ValueLayout.JAVA_BYTE);
    }
}