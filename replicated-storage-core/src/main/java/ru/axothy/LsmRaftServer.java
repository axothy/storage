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
import java.util.UUID;

public final class LsmRaftServer {

    public static void main(String[] args) throws Exception {
        RaftPeerId       self   = RaftPeerId.valueOf(args[0]);          // "n1"
        RaftGroupId      gid    = RaftGroupId.valueOf(UUID.fromString(args[1]));
        List<RaftPeer>   peers  = parsePeers(args[2]);                  // "n1:localhost:8761,n2:…"
        RaftProperties   props  = new RaftProperties();

        int port = Integer.parseInt(args[3]);                           // 8761
        GrpcConfigKeys.Server.setPort(props, port);                     // gRPC транспорт :contentReference[oaicite:1]{index=1}

        Path dataDir = Paths.get(args[4]);                              // "/data/n1"
        RaftServerConfigKeys.setStorageDir(props, List.of(dataDir.toFile()));

        StateMachine sm = new LsmStateMachine(dataDir.resolve("lsm"));
        RaftGroup    grp = RaftGroup.valueOf(gid, peers);

        RaftServer.newBuilder()
                .setServerId(self)
                .setGroup(grp)
                .setStateMachine(sm)
                .setProperties(props)
                .build()
                .start();
    }

    private static List<RaftPeer> parsePeers(String csv) {
        return Arrays.stream(csv.split(","))
                .map(seg -> {                         // "n1:localhost:8761"
                    String[] p   = seg.split(":");
                    String  id   = p[0];
                    String  host = p[1] + ':' + p[2]; // "localhost:8761"

                    return RaftPeer.newBuilder()      // ← актуальный API Ratis-3.x
                            .setId(RaftPeerId.valueOf(id))
                            .setAddress(host)
                            .build();
                })
                .toList();
    }
}

