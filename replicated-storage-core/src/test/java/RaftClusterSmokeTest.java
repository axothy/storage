import lsmraft.Lsmraft;
import org.apache.ratis.protocol.*;
import org.apache.ratis.client.*;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.*;
import ru.axothy.LsmStateMachine;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static ru.axothy.utils.ProtoEntryConverters.parse;
import static ru.axothy.utils.ProtoEntryConverters.toMessage;

public class RaftClusterSmokeTest {
    private static final RaftGroupId GID = RaftGroupId.valueOf(UUID.randomUUID());

    private final List<RaftPeer> peers = new ArrayList<>();

    private final List<RaftServer> servers = new ArrayList<>();

    private final List<Path> dataDirs = new ArrayList<>();

    @BeforeEach
    void start() throws Exception {
        for (int i = 1; i <= 3; i++) {
            int port = freePort();
            String id = "n" + i;
            RaftPeer peer = RaftPeer.newBuilder()
                    .setId(RaftPeerId.valueOf(id))
                    .setAddress("localhost:" + port)
                    .build();
            peers.add(peer);

            Path dir = Files.createTempDirectory("lsmraft-" + id);
            dataDirs.add(dir);

            RaftProperties props = new RaftProperties();
            GrpcConfigKeys.Server.setPort(props, port);

            RaftServer srv = RaftServer.newBuilder()
                    .setServerId(peer.getId())
                    .setGroup(RaftGroup.valueOf(GID, peers))
                    .setStateMachine(new LsmStateMachine(dir))
                    .setProperties(props)
                    .build();
            servers.add(srv);
            srv.start();
        }
        waitLeader();
    }

    @AfterEach
    void stop() throws Exception {
        for (RaftServer s : servers) {
            s.close();
        }

        for (Path p : dataDirs) {
            deleteRecursive(p);
        }
    }

    /* ---------- TEST-0: кластер поднялся ---------- */
    @Test
    void clusterIsUpAndLeaderElected() {
        boolean leaderFound = servers.stream()
                .anyMatch(s -> {
                    try {
                        return s.getDivision(GID).getInfo().isLeader();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
        assertTrue(leaderFound, "leader must exist");
    }

    /* ---------- ТЕСТ 1: upsert / get ---------- */
    @Test
    void upsertAndGet() throws Exception {
        try (RaftClient client = newClient()) {
            byte[] key = "k1".getBytes();
            byte[] val = "v1".getBytes();

            var cmd = Lsmraft.Command.newBuilder()
                    .setUpsert(Lsmraft.Upsert.newBuilder()
                            .setEntry(Lsmraft.KvEntry.newBuilder()
                                    .setKey(com.google.protobuf.ByteString.copyFrom(key))
                                    .setValue(com.google.protobuf.ByteString.copyFrom(val))))
                    .build();

            RaftClientReply wrep = client.io().send(toMessage(cmd));
            assertTrue(wrep.isSuccess());

            var q = Lsmraft.Query.newBuilder()
                    .setGet(Lsmraft.Get.newBuilder()
                            .setKey(com.google.protobuf.ByteString.copyFrom(key)))
                    .build();

            RaftClientReply rrep = client.io().sendReadOnly(toMessage(q));
            assertTrue(rrep.isSuccess());

            Lsmraft.QueryReply qr = parse(rrep.getMessage(), Lsmraft.QueryReply.parser());
            assertArrayEquals(val, qr.getSingle().getValue().toByteArray());
        }
    }

    /* ---------- ТЕСТ 2: лидер переживает kill-9 узла ---------- */
    @Test
    void survivesOneNodeDown() throws Exception {
        servers.get(0).close();                       // выключаем первый
        try (RaftClient client = newClient()) {
            Lsmraft.Query q = Lsmraft.Query.newBuilder()
                    .setGet(Lsmraft.Get.newBuilder()
                            .setKey(com.google.protobuf.ByteString.copyFromUtf8("none")))
                    .build();
            RaftClientReply r = client.io().sendReadOnly(toMessage(q));
            assertTrue(r.isSuccess());
        }
    }

    /* ---------- helpers ---------- */
    private static int freePort() throws Exception {
        try (var s = new java.net.ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private void waitLeader() throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            for (RaftServer s : servers) {
                if (s.getDivision(GID).getInfo().isLeader()) return;
            }
            Thread.sleep(200);
        }
        fail("Leader not elected");
    }

    private RaftClient newClient() throws Exception {
        return RaftClient.newBuilder()
                .setRaftGroup(RaftGroup.valueOf(GID, peers))
                .setProperties(new RaftProperties())
                .build();
    }

    private static void deleteRecursive(Path p) {
        try {
            Files.walk(p)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try { Files.deleteIfExists(path); } catch (Exception ignore) {}
                    });
        } catch (Exception ignore) {}
    }
}
