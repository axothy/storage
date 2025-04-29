package ru.axothy;

import com.google.protobuf.ByteString;
import lsmraft.Lsmraft;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.ProtoUtils;
import ru.axothy.api.Entry;
import ru.axothy.api.Storage;
import ru.axothy.config.Config;
import ru.axothy.storage.BaseEntry;
import ru.axothy.storage.LSMStorage;
import ru.axothy.utils.ProtoEntryConverters;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public final class LsmStateMachine extends BaseStateMachine {

    private static final long FLUSH_THRESHOLD_BYTES = 4_194_3040L; //fixme вынос в конфиг

    private final Storage<MemorySegment, Entry<MemorySegment>> delegate;

    public LsmStateMachine(Path baseDir) {
        this.delegate = new LSMStorage(new Config(baseDir, FLUSH_THRESHOLD_BYTES, 0.2));
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        try {
            Lsmraft.Command command = Lsmraft.Command.parseFrom(trx.getLogEntry().getStateMachineLogEntry().getLogData().toByteArray());

            switch (command.getPayloadCase()) {
                case UPSERT -> {
                    Lsmraft.KvEntry kv = command.getUpsert().getEntry();

                    upsert(kv);
                    return CompletableFuture.completedFuture(Message.EMPTY);
                }
                case DEL -> {
                    delete(command.getDel().getKey());
                    return CompletableFuture.completedFuture(Message.EMPTY);
                }
                default -> throw new IllegalStateException("Unknown command");
            }
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        try {
            Lsmraft.Query query = Lsmraft.Query.parseFrom(request.getContent().toByteArray());
            return switch (query.getPayloadCase()) {
                case GET -> CompletableFuture.completedFuture(singleGet(query.getGet()));
                case RANGE -> CompletableFuture.completedFuture(rangeGet(query.getRange()));
                default -> CompletableFuture.failedFuture(new IllegalArgumentException("Unknown query"));
            };
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private void upsert(Lsmraft.KvEntry proto) {
        delegate.upsert(
                new BaseEntry<>(
                        ProtoEntryConverters.toSegment(proto.getKey().toByteArray()),
                        ProtoEntryConverters.toSegment(proto.getValue().toByteArray())
                )
        );
    }

    private void delete(ByteString tombstoneKey) {
        MemorySegment key = ProtoEntryConverters.toSegment(tombstoneKey.toByteArray());
        delegate.upsert(new BaseEntry<>(key, null));
    }

    private Message singleGet(Lsmraft.Get get) {
        MemorySegment key = ProtoEntryConverters.toSegment(get.getKey());
        Entry<MemorySegment> entry = delegate.get(key);

        Lsmraft.QueryReply.Builder reply = Lsmraft.QueryReply.newBuilder();
        if (entry != null) {
            reply.setSingle(ProtoEntryConverters.toProtoEntry(entry));
        }

        return Message.valueOf(ProtoUtils.toByteString(reply.build().toByteArray()));
    }

    private Message rangeGet(Lsmraft.Range range) {
        MemorySegment from = ProtoEntryConverters.toSegment(range.getFrom());
        MemorySegment to = ProtoEntryConverters.toSegment(range.getTo());
        Iterator<Entry<MemorySegment>> iterator = delegate.get(from, to);

        Lsmraft.QueryReply.EntryIteratorChunk.Builder chunk = Lsmraft.QueryReply.EntryIteratorChunk.newBuilder();
        for (int i = 0; i < 256 & iterator.hasNext(); i++) {
            chunk.addEntries(ProtoEntryConverters.toProtoEntry(iterator.next()));
        }

        chunk.setFinished(!iterator.hasNext());

        Lsmraft.QueryReply.Builder reply = Lsmraft.QueryReply.newBuilder();
        reply.setChunk(chunk);

        return Message.valueOf(ProtoUtils.toByteString(reply.build().toByteArray()));
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public long takeSnapshot() throws IOException {
        // TODO implement
        return 0;
    }

}
