package ru.axothy;

import com.google.protobuf.InvalidProtocolBufferException;
import lsmraft.Lsmraft;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.*;
import ru.axothy.api.Entry;
import ru.axothy.api.Storage;
import ru.axothy.storage.BaseEntry;
import ru.axothy.utils.ProtoEntryConverters;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.*;

public final class ReplicatedStorage implements Storage<MemorySegment, Entry<MemorySegment>>, Closeable {

    private final RaftClient client;

    public ReplicatedStorage(RaftGroupId gid, Collection<RaftPeer> peers) {
        RaftProperties props = new RaftProperties();

        client = RaftClient.newBuilder()
                .setRaftGroup(RaftGroup.valueOf(gid, peers))
                .setProperties(props)
                .setClientRpc(new GrpcFactory().newRaftClientRpc(props))
                .build();
    }

    @Override
    public void upsert(Entry<MemorySegment> e) {
        Lsmraft.Command cmd = Lsmraft.Command.newBuilder()
                .setUpsert(Lsmraft.Upsert.newBuilder()
                .setEntry(ProtoEntryConverters.toProtoEntry(e)))
                .build();

        send(Message.valueOf(cmd.toByteString()));
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Lsmraft.Query q = Lsmraft.Query.newBuilder()
                .setGet(Lsmraft.Get.newBuilder()
                .setKey(ProtoEntryConverters.toBytes(key)))
                .build();

        byte[] raw = sendReadOnly(Message.valueOf(q.toByteString())).getMessage().getContent().toByteArray();

        Lsmraft.QueryReply rep = null;
        try {
            rep = Lsmraft.QueryReply.parseFrom(raw);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Cannot decode Command", e);
        }

        if (rep.hasSingle()) {
            return new BaseEntry<>(
                    ProtoEntryConverters.toSegment(rep.getSingle().getKey().toByteArray()),
                    ProtoEntryConverters.toSegment(rep.getSingle().getValue().toByteArray())
            );
        } else {
            return null;
        }
    }

    /* -------  range: лениво подкачиваем по частям из leader ------- */
    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        Lsmraft.Query query = Lsmraft.Query.newBuilder()
                .setRange(Lsmraft.Range.newBuilder()
                .setFrom(ProtoEntryConverters.toBytes(from))
                .setTo(ProtoEntryConverters.toBytes(to)))
                .build();

        try {
            // Ratis вернёт stream сообщений; превращаем в итератор
            RaftClientReply first = client.io().sendReadOnly(Message.valueOf(query.toByteArray()));

            return new Iterator<>() {
                Deque<Entry<MemorySegment>> buf = new ArrayDeque<>();
                boolean finished = false;

                @Override
                public boolean hasNext() {
                    preload();
                    return !buf.isEmpty();
                }

                @Override
                public Entry<MemorySegment> next() {
                    preload();
                    if (buf.isEmpty()) throw new NoSuchElementException();
                    return buf.removeFirst();
                }

                private void preload() {
                    if (!buf.isEmpty() || finished) return;
                    try {
                        Lsmraft.QueryReply rep = Lsmraft.QueryReply.parseFrom(first.getMessage().getContent());
                        var chunk = rep.getChunk();
                        chunk.getEntriesList().forEach(e ->
                                buf.addLast(
                                        new BaseEntry<>(
                                                ProtoEntryConverters.toSegment(e.getKey().toByteArray()),
                                                ProtoEntryConverters.toSegment(e.getValue().toByteArray()))
                                )
                        );

                        finished = chunk.getFinished();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override public Iterator<Entry<MemorySegment>> all() {
        return get(null, null);
    }

    @Override public void flush() {

    }

    @Override public void compact() {

    }

    @Override public void close() throws IOException {
        client.close();
    }

    private RaftClientReply send(Message m) {
        try {
            return client.io().send(m);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private RaftClientReply sendReadOnly(Message m) {
        try {
            return client.io().sendReadOnly(m);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}

