package ru.axothy;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.NetUtils;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class LsmRaftServer {

    public static void main(String[] args) throws Exception {
        String id = System.getenv("NODE_ID");
        String peersEnv = System.getenv("PEERS"); // "n1:localhost:22001,n2:localhost:22002,..."
        Path dataDir = Path.of("/var/lib/lsm/" + id);

        RaftPeerId serverId = RaftPeerId.valueOf(id);

        List<RaftPeer> peers = Stream.of(peersEnv.split(","))
                .map(s -> {
                    String[] p = s.split(":");
                    RaftPeerId peerId = RaftPeerId.valueOf(p[0]);
                    InetSocketAddress addr = NetUtils.createSocketAddr(p[1],
                            Integer.parseInt(p[2]));
                    return RaftPeer.newBuilder()
                            .setId(peerId)
                            .setAddress(addr)
                            .build();
                })
                .collect(Collectors.toList());

        RaftGroupId groupId = RaftGroupId.valueOf(UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"));
        RaftGroup group = RaftGroup.valueOf(groupId, peers);

        RaftProperties props = new RaftProperties();
        GrpcConfigKeys.Server.setPort(props, Integer.parseInt(System.getenv("PORT")));

        LsmStateMachine sm = new LsmStateMachine(dataDir);
        RaftServer server = RaftServer.newBuilder()
                .setServerId(serverId)
                .setStateMachine(sm)
                .setProperties(props)
                .setGroup(group)
                .setParameters(GrpcFactory.newRaftParameters(null)) //fixme tls null
                .build();

        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.close();
            } catch (Exception ignored) { }
        }));
    }
}

