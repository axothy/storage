package ru.axothy;

import com.google.protobuf.InvalidProtocolBufferException;
import lsmraft.Lsmraft;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
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

    private static final long FLUSH_THRESHOLD_BYTES = 4_194_3040L;

    private final Storage<MemorySegment, Entry<MemorySegment>> delegate;

    public LsmStateMachine(Path baseDir) {
        this.delegate = new LSMStorage(new Config(baseDir, FLUSH_THRESHOLD_BYTES, 0.2));
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {

        Lsmraft.Command cmd;
        try {
            cmd = Lsmraft.Command.parseFrom(trx.getLogEntry().getStateMachineLogEntry().getLogData().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Cannot decode Command", e);
        }

        switch (cmd.getPayloadCase()) {
            case UPSERT -> {
                Lsmraft.KvEntry kv = cmd.getUpsert().getEntry();

                delegate.upsert(
                        new BaseEntry<>(
                                ProtoEntryConverters.toSegment(kv.getKey().toByteArray()),
                                ProtoEntryConverters.toSegment(kv.getValue().toByteArray())
                        )
                );
            } //fixme ???
            case DEL -> {
                MemorySegment key = ProtoEntryConverters.toSegment(cmd.getDel().getKey().toByteArray());
                delegate.upsert(new BaseEntry<>(key, null));
            }
            default -> throw new IllegalStateException("unknown cmd");
        }

        return CompletableFuture.completedFuture(Message.valueOf("OK"));
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        try {
            Lsmraft.Query query = Lsmraft.Query.parseFrom(request.getContent().toByteArray());
            Lsmraft.QueryReply.Builder reply = Lsmraft.QueryReply.newBuilder();

            switch (query.getPayloadCase()) {
                case GET -> {
                    MemorySegment key = ProtoEntryConverters.toSegment(query.getGet().getKey());
                    Entry<MemorySegment> entry = delegate.get(key);
                    if (entry != null) {
                        reply.setSingle(ProtoEntryConverters.toProtoEntry(entry));
                    }

                    return completed(reply);
                } //fixme ???

                case RANGE -> {
                    MemorySegment from = ProtoEntryConverters.toSegment(query.getRange().getFrom());
                    MemorySegment to = ProtoEntryConverters.toSegment(query.getRange().getTo());
                    Iterator<Entry<MemorySegment>> iterator = delegate.get(from, to);

                    Lsmraft.QueryReply.EntryIteratorChunk.Builder chunk = Lsmraft.QueryReply.EntryIteratorChunk.newBuilder();
                    for (int i = 0; i < 256 & iterator.hasNext(); i++) {
                        chunk.addEntries(ProtoEntryConverters.toProtoEntry(iterator.next()));
                    }

                    chunk.setFinished(!iterator.hasNext());
                    reply.setChunk(chunk);
                    return completed(reply);
                }

                default -> throw new IllegalStateException("unknown query");
            }
        } catch (IOException ex) {
            return failed(ex);
        }
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    private static CompletableFuture<Message> completed(Lsmraft.QueryReply.Builder builder) {
        return CompletableFuture.completedFuture(
                Message.valueOf(builder.build().toByteString())
        );
    }

    private static CompletableFuture<Message> failed(Throwable t) {
        CompletableFuture<Message> cf = new CompletableFuture<>();
        cf.completeExceptionally(t);

        return cf;
    }
}
