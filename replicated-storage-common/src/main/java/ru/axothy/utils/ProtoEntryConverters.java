package ru.axothy.utils;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import lsmraft.*;
import org.apache.ratis.protocol.Message;
import com.google.protobuf.Parser;
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

    public static Message toMessage(MessageLite src) {
        byte[] data = src.toByteArray();
        return Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(data));
    }

    public static org.apache.ratis.thirdparty.com.google.protobuf.ByteString toByteString(MessageLite src) {
        byte[] data = src.toByteArray();
        return org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(data);
    }

    public static <T extends MessageLite> T parse(Message ratisMsg, Parser<T> parser) throws InvalidProtocolBufferException {
        return parser.parseFrom(ratisMsg.getContent().toByteArray());
    }
}