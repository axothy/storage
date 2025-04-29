import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.axothy.LsmStateMachine;

import java.nio.file.Files;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class OneNodeSimpleClusterTest {
    private static final RaftGroupId GID = RaftGroupId.valueOf(UUID.randomUUID());
    private static RaftServer server;

    @BeforeAll
    static void startOneNode() throws Exception {
        int port = freePort();
        RaftPeer peer = RaftPeer.newBuilder()
                .setId(RaftPeerId.valueOf("n1"))
                .setAddress("localhost:" + port)
                .build();
        RaftProperties p = new RaftProperties();
        GrpcConfigKeys.Server.setPort(p, port);

        server = RaftServer.newBuilder()
                .setServerId(peer.getId())
                .setGroup(RaftGroup.valueOf(GID, List.of(peer)))
                .setStateMachine(new LsmStateMachine(Files.createTempDirectory("lsmraft-single")))
                .setProperties(p)
                .build();
        server.start();
    }

    @AfterAll
    static void stop() throws Exception {
        server.close();
    }

    @Test
    void serverIsRunning() {
        assertTrue(server.getLifeCycleState().isRunning());
    }

    private static int freePort() throws Exception {
        try (var s = new java.net.ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}

