package de.kbartz;

import org.junit.jupiter.api.Test;
import org.oxoo2a.sim4da.Simulator;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSimpleElection {

    //    Achtung: die Tests laufen nur einzeln, nicht hintereinander durch. Das liegt wahrscheinlich an der simulator Instanz, die wiederverwendet wird
    @Test
    void testSimpleElection() {
        ArrayList<String> members = new ArrayList<>();
        members.add("fast");
        members.add("slow");
        Server s1 = new Server("fast", 50, members, false);
        Server s3 = new Server("slow", 150, members, false);
        Simulator sim = Simulator.getInstance();
        sim.simulate(1);
        sim.shutdown();
        assertEquals(ServerType.LEADER, s1.getServerType());
        assertEquals(ServerType.FOLLOWER, s3.getServerType());
    }

    @Test
    void testDifferentTimeouts() {
        ArrayList<String> cluster = new ArrayList<>();
        cluster.add("fast");
        cluster.add("fast2");
        cluster.add("slow");
        cluster.add("slow2");
        Server s1 = new Server("fast", 49, cluster, false);
        Server s2 = new Server("fast2", 50, cluster, false);
        Server s3 = new Server("slow", 150, cluster, false);
        Server s4 = new Server("slow2", 150, cluster, false);
        Simulator sim = Simulator.getInstance();
        sim.simulate(1);
        sim.shutdown();
        assert s1.getServerType() == ServerType.LEADER || s2.getServerType() == ServerType.LEADER;
        assertEquals(ServerType.FOLLOWER, s3.getServerType());
        assertEquals(ServerType.FOLLOWER, s4.getServerType());
    }
}
