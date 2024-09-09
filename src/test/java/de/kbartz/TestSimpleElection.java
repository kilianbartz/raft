package de.kbartz;

import org.junit.jupiter.api.Test;
import org.oxoo2a.sim4da.Simulator;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

public class TestSimpleElection {

    @Test
    void testSimpleElection() {
        ArrayList<String> members = new ArrayList<>();
        members.add("fast");
        members.add("slow");
        Server s1 = new Server("fast", 10, members);
        Server s2 = new Server("slow", 30, members);
        Simulator sim = Simulator.getInstance();
        sim.simulate(1);
        sim.shutdown();
        assertEquals(ServerType.LEADER, s1.getServerType());
        assertEquals(ServerType.FOLLOWER, s2.getServerType());
    }
}
