package de.kbartz;

import org.junit.jupiter.api.Test;
import org.oxoo2a.sim4da.Simulator;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestClientAppends {

//    TODO: test overwriting

    //    Achtung: die Tests laufen nur einzeln, nicht hintereinander durch. Das liegt wahrscheinlich an der simulator Instanz, die wiederverwendet wird
    @Test
    void testSimpleAppend() {
        ArrayList<String> members = new ArrayList<>();
        members.add("fast");
        members.add("slow");
        Server s1 = new Server("fast", 50, members);
        Server s3 = new Server("slow", 150, members);
        ArrayList<LogEntry> log = new ArrayList<>();
        log.add(new LogEntry("x", "1"));
        log.add(new LogEntry("y", "2"));
        TestClient client = new TestClient("client", log, "fast");
        Simulator sim = Simulator.getInstance();
        sim.simulate(1);
        sim.shutdown();
        assertEquals(ServerType.LEADER, s1.getServerType());
        assertEquals(2, s1.getLog().size());
        LogEntry e = s1.getLog().getFirst();
        assertEquals(1, e.getTerm());
        assertEquals("x", e.getKey());
        assertEquals("1", e.getValue());
        e = s1.getLog().getLast();
        assertEquals(1, e.getTerm());
        assertEquals("y", e.getKey());
        assertEquals("2", e.getValue());
        assertEquals(2, s3.getLog().size());
        e = s3.getLog().getFirst();
        assertEquals(1, e.getTerm());
        assertEquals("x", e.getKey());
        assertEquals("1", e.getValue());
        e = s3.getLog().getLast();
        assertEquals(1, e.getTerm());
        assertEquals("y", e.getKey());
        assertEquals("2", e.getValue());
        assertEquals(1, s1.getCommitIndex());
        assertEquals(1, s3.getCommitIndex());
        assertEquals("1", s1.getStateMachine().get("x"));
        assertEquals("2", s1.getStateMachine().get("y"));
        assertEquals("1", s3.getStateMachine().get("x"));
        assertEquals("2", s3.getStateMachine().get("y"));
        assertEquals(1, client.successes);
    }
}
