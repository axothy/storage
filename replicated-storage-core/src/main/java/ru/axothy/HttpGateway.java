package ru.axothy;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.javalin.Javalin;
import io.javalin.http.BadRequestResponse;
import io.javalin.http.Context;
import lsmraft.Lsmraft;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import ru.axothy.utils.ProtoEntryConverters;

import java.io.IOException;
import java.util.Base64;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class HttpGateway {
    private static final Base64.Encoder B64E = Base64.getEncoder();

    private static final Base64.Decoder B64D = Base64.getUrlDecoder();

    private static final int POOL_SIZE = 20;

    private static final int QUEUE_CAPACITY = 256;

    public static void start(int port, RaftGroup group, RaftProperties props) {
        RaftClient client = RaftClient.newBuilder()
                .setRaftGroup(group)
                .setProperties(props)
                .build();

        ExecutorService executor = new ThreadPoolExecutor(
                POOL_SIZE,
                POOL_SIZE,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(QUEUE_CAPACITY)
        );

        Javalin app = Javalin.create(cfg -> cfg.showJavalinBanner = false)
                .events(event -> event.serverStopped(() -> {
                    executor.shutdownNow();
                    client.close();
                }))
                .start(port);

        app.get("/v0/entity", ctx ->
                async(ctx, executor, () -> {
                    byte[] key = getParameterId(ctx);

                    Lsmraft.Query query = Lsmraft.Query.newBuilder()
                            .setGet(Lsmraft.Get.newBuilder().setKey(ByteString.copyFrom(key)))
                            .build();

                    RaftClientReply reply;
                    try {
                        reply = client.io().sendReadOnly(ProtoEntryConverters.toMessage(query));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    Lsmraft.QueryReply queryReply;
                    try {
                        queryReply = ProtoEntryConverters.parse(reply.getMessage(), Lsmraft.QueryReply.parser());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException(e);
                    }

                    if (!queryReply.hasSingle()) {
                        ctx.status(404);
                    } else {
                        ctx.status(200).result(queryReply.getSingle().getValue().toByteArray());
                    }
                })
        );

        app.put("/v0/entity", ctx ->
                async(ctx, executor, () -> {
                    byte[] key = getParameterId(ctx);
                    byte[] value = ctx.bodyAsBytes();

                    Lsmraft.Command command = Lsmraft.Command.newBuilder()
                            .setUpsert(Lsmraft.Upsert.newBuilder()
                                    .setEntry(Lsmraft.KvEntry.newBuilder()
                                            .setKey(ByteString.copyFrom(key))
                                            .setValue(ByteString.copyFrom(value))))
                            .build();

                    RaftClientReply reply;
                    try {
                        reply = client.io().send(ProtoEntryConverters.toMessage(command));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    if (reply.isSuccess()) {
                        ctx.status(201);
                    } else {
                        throw new RuntimeException(reply.getException());
                    }
                }));

        app.delete("/v0/entity", ctx ->
                async(ctx, executor, () -> {
                    byte[] key = getParameterId(ctx);

                    Lsmraft.Command cmd = Lsmraft.Command.newBuilder()
                            .setDel(Lsmraft.Delete.newBuilder()
                                    .setKey(ByteString.copyFrom(key)))
                            .build();

                    RaftClientReply reply;
                    try {
                        reply = client.io().send(ProtoEntryConverters.toMessage(cmd));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    if (reply.isSuccess()) {
                        ctx.status(202);
                    } else {
                        throw new RuntimeException(reply.getException());
                    }
                }));

        app.exception(RejectedExecutionException.class, (_, ctx) -> ctx.status(503).result("ExecutorService queue overflow"));
        app.exception(Exception.class, (e, ctx) -> ctx.status(500).result(e.toString())); //fixme
    }

    private static void async(Context ctx, ExecutorService pool, Runnable handler) {
        ctx.future(() -> CompletableFuture
                .runAsync(() -> safe(handler, ctx), pool));
    }

    private static void safe(Runnable r, Context ctx) {
        try {
            r.run();
        } catch (RejectedExecutionException e) {
            ctx.status(503).result("ExecutorService queue overflow");
        } catch (Throwable e) {
            ctx.status(500);
        }
    }

    private static byte[] getParameterId(Context ctx) {
        String id = ctx.queryParam("id");

        if (id == null || id.isBlank()) {
            throw new BadRequestResponse("id param required");
        }

        return B64D.decode(id);
    }

}
