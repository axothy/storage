package ru.axothy;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import lsmraft.Lsmraft;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;
import ru.axothy.api.Entry;
import ru.axothy.api.Storage;
import ru.axothy.storage.BaseEntry;
import ru.axothy.utils.ProtoEntryConverters;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.*;

public final class DistributedStorage implements Storage<MemorySegment, Entry<MemorySegment>>, Closeable {

    private final RaftClient client;

    private DistributedStorage(RaftGroup group, RaftProperties props) throws IOException {
        this.client = RaftClient.newBuilder()
                .setRaftGroup(group)
                .setProperties(props)
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final List<RaftPeer> peers = new ArrayList<>();

        private UUID groupId;

        private RaftProperties props = new RaftProperties();

        public Builder groupId(UUID gid) {
            this.groupId = gid;
            return this;
        }

        public Builder addPeer(String id, String addr) {
            peers.add(RaftPeer.newBuilder()
                    .setId(RaftPeerId.valueOf(id))
                    .setAddress(addr)           // "host:port"
                    .build());
            return this;
        }

        public Builder properties(RaftProperties p) {
            this.props = p;
            return this;
        }

        public DistributedStorage build() throws IOException {
            return new DistributedStorage(
                    RaftGroup.valueOf(RaftGroupId.valueOf(groupId), peers), props);
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        Lsmraft.Command command = Lsmraft.Command.newBuilder()
                .setUpsert(Lsmraft.Upsert.newBuilder().setEntry(ProtoEntryConverters.toProtoEntry(entry)))
                .build();
        try {
            client.io().send(Message.valueOf(parse(command)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Lsmraft.Query q = Lsmraft.Query.newBuilder()
                .setGet(Lsmraft.Get.newBuilder().setKey(ByteString.copyFrom(key.asByteBuffer())))
                .build();
        RaftClientReply reply;
        try {
            reply = client.io().sendReadOnly(Message.valueOf(parse(q)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Message smReply = reply.getMessage();

        Lsmraft.QueryReply qr;
        try {
            qr = parse(smReply, Lsmraft.QueryReply.parser());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        return new BaseEntry<>(
                ProtoEntryConverters.toSegment(qr.getSingle().getKey().toByteArray()),
                ProtoEntryConverters.toSegment(qr.getSingle().getValue().toByteArray())
        );
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return new RemoteIterator(from, to);
    }

    private final class RemoteIterator implements Iterator<Entry<MemorySegment>> {
        private Queue<Entry<MemorySegment>> buf = new ArrayDeque<>();

        private boolean finished = false;

        private final MemorySegment from, to;

        RemoteIterator(MemorySegment from, MemorySegment to) {
            this.from = from;
            this.to = to;
            fetch();
        }

        private void fetch() {
            if (finished) return;
            Lsmraft.Query q = Lsmraft.Query.newBuilder()
                    .setRange(Lsmraft.Range.newBuilder()
                            .setFrom(ByteString.copyFrom(from.asByteBuffer()))
                            .setTo(ByteString.copyFrom(to.asByteBuffer())))
                    .build();
            RaftClientReply reply;
            try {
                reply = client.io().sendReadOnly(Message.valueOf(parse(q)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            Message smReply = reply.getMessage();

            Lsmraft.QueryReply qr;
            try {
                qr = parse(smReply, Lsmraft.QueryReply.parser());
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
            Lsmraft.QueryReply.EntryIteratorChunk ch = qr.getChunk();
            ch.getEntriesList().forEach(e -> buf.add(
                    new BaseEntry<>(
                            ProtoEntryConverters.toSegment(e.getKey().toByteArray()),
                            ProtoEntryConverters.toSegment(e.getValue().toByteArray())
                    )));
            finished = ch.getFinished();
        }

        @Override public boolean hasNext() {
            if (buf.isEmpty()) fetch();
            return !buf.isEmpty();
        }

        @Override public Entry<MemorySegment> next() {
            if (!hasNext()) throw new NoSuchElementException();
            return buf.poll();
        }
    }

    @Override public void close() throws IOException {
        client.close();
    }

    private org.apache.ratis.thirdparty.com.google.protobuf.ByteString parse(MessageLite src) {
        byte[] data = src.toByteArray();
        return org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(data);
    }

    public static <T extends MessageLite> T parse(Message ratisMsg, Parser<T> parser) throws InvalidProtocolBufferException {
        return parser.parseFrom(ratisMsg.getContent().toByteArray());
    }

}
