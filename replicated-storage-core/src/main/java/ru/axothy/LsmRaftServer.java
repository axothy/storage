package ru.axothy;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.StateMachine;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public final class LsmRaftServer {

    public static void main(String[] args) throws Exception {
        String selfId = argOrEnv(args, 0, "PEER_ID",  "n1");
        String gidStr = argOrEnv(args, 1, "GROUP_ID", "9fd7bc90-88f0-4ab6-aedd-5e0d182e027f");
        String peersCsv = argOrEnv(args, 2, "PEERS", selfId + ":localhost:8761");
        int port = Integer.parseInt(argOrEnv(args, 3, "PORT", "8761"));
        String data = argOrEnv(args, 4, "DATA_DIR", "/data");

        /* -------- configs -------- */
        RaftProperties properties = new RaftProperties();
        GrpcConfigKeys.Server.setPort(properties, port);
        Path dataDir = Paths.get(data);
        RaftServerConfigKeys.setStorageDir(properties, List.of(dataDir.toFile()));

        StateMachine stateMachine = new LsmStateMachine(dataDir.resolve("lsm"));
        RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.fromString(gidStr)), parsePeers(peersCsv));

        RaftServer server = RaftServer.newBuilder()
                .setServerId(RaftPeerId.valueOf(selfId))
                .setGroup(raftGroup)
                .setStateMachine(stateMachine)
                .setProperties(properties)
                .build();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.close();
            } catch (Exception ignore) {}
        }));

        server.start();
    }

    private static String argOrEnv(String[] args, int idx, String env, String dflt) {
        if (args.length > idx) {
            return args[idx];
        }

        return Optional.ofNullable(System.getenv(env)).orElse(dflt);
    }

    private static List<RaftPeer> parsePeers(String csv) {
        return Arrays.stream(csv.split(","))
                .map(seg -> {
                    String[] p = seg.split(":");
                    return RaftPeer.newBuilder()
                            .setId(RaftPeerId.valueOf(p[0]))
                            .setAddress(p[1] + ':' + p[2])
                            .build();
                })
                .toList();
    }
}
