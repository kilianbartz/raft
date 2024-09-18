package de.kbartz;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.oxoo2a.sim4da.Message;
import org.oxoo2a.sim4da.Simulator;

import java.util.ArrayList;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestClientAppends {

    private final static ObjectMapper mapper = new ObjectMapper();

    public static Message convertToMessage(LogEntry[] logs) {
        return convertToMessage(logs, UUID.randomUUID().toString());
    }

    public static Message convertToMessage(LogEntry[] logs, String uuid) {
        Message m = new Message();
        m.addHeader("type", "clientPut");
        try {
            m.add("entries", mapper.writeValueAsString(logs));
            m.add("uuid", uuid);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return m;
    }


    //    Achtung: die Tests laufen nur einzeln, nicht hintereinander durch. Das liegt wahrscheinlich an der simulator Instanz, die wiederverwendet wird
    @Test
    void testSimpleAppend() {
        ArrayList<String> members = new ArrayList<>();
        members.add("fast");
        members.add("slow");
        Server s1 = new Server("fast", 50, members);
        Server s3 = new Server("slow", 150, members);
        LogEntry[] logs = new LogEntry[]{new LogEntry("x", "1"), new LogEntry("y", "2")};
        TestClient client = new TestClient("client", new Message[]{convertToMessage(logs)}, "fast", members);
        Simulator sim = Simulator.getInstance();
        sim.simulate(1);
        sim.shutdown();
        assertEquals(ServerType.LEADER, s1.getServerType());
        assertEquals(3, s1.getLog().size());
//        first is noop
        LogEntry e = s1.getLog().getFirst();
        assertEquals(1, e.getTerm());
        assertEquals("noop", e.getKey());
        assertEquals("x", s1.getLog().get(1).getKey());
        assertEquals("1", s1.getLog().get(1).getValue());
        e = s1.getLog().getLast();
        assertEquals(1, e.getTerm());
        assertEquals("y", e.getKey());
        assertEquals("2", e.getValue());
        assertEquals(3, s3.getLog().size());
        e = s3.getLog().getLast();
        assertEquals(1, e.getTerm());
        assertEquals("y", e.getKey());
        assertEquals("2", e.getValue());
        assertEquals(2, s1.getCommitIndex());
        assertEquals(2, s3.getCommitIndex());
        assertEquals("1", s1.getStateMachine().get("x"));
        assertEquals("2", s1.getStateMachine().get("y"));
        assertEquals("1", s3.getStateMachine().get("x"));
        assertEquals("2", s3.getStateMachine().get("y"));
        assertEquals(1, client.successes);
    }

    @Test
    void testOverwritingFollowerLog() {
        ArrayList<String> members = new ArrayList<>();
        members.add("fast");
        members.add("slow");
        Server s1 = new Server("fast", 50, members);
        Server s3 = new Server("slow", 150, members);
        Simulator sim = Simulator.getInstance();
        LogEntry e = new LogEntry(1, "x", "1");
        s1.getLog().add(e);
        s3.getLog().add(new LogEntry(0, "y", "2"));
        sim.simulate(1);
        sim.shutdown();
        assertEquals(ServerType.LEADER, s1.getServerType());
        assertEquals(ServerType.FOLLOWER, s3.getServerType());
        assertEquals(s1.getLog().getFirst(), s3.getLog().getFirst());
    }

    @Test
    void testSameUuid() {
        ArrayList<String> members = new ArrayList<>();
        members.add("fast");
        members.add("slow");
        Server s1 = new Server("fast", 50, members);
        Server s3 = new Server("slow", 150, members);
        LogEntry[] logs = new LogEntry[]{new LogEntry("x", "1"), new LogEntry("y", "2")};
        Message first = convertToMessage(logs, "asdf");
        Message[] messages = new Message[]{first, first, convertToMessage(logs, "different")};
        TestClient client = new TestClient("client", messages, "fast", members);
        Simulator sim = Simulator.getInstance();
        sim.simulate(2);
        sim.shutdown();
        assertEquals(3, client.successes);
        assertEquals(5, s1.getLog().size());
    }

    @Test
    public void testClientGeta() {
        ArrayList<String> members = new ArrayList<>();
        members.add("fast");
        members.add("slow");
        Server s1 = new Server("fast", 50, members);
        Server s3 = new Server("slow", 150, members);
        LogEntry[] logs = new LogEntry[]{new LogEntry("x", "1"), new LogEntry("y", "2")};
        Message getMessage = new Message();
        getMessage.addHeader("type", "clientGet");
        getMessage.add("key", "x");
        TestClient client = new TestClient("client", new Message[]{convertToMessage(logs), getMessage}, "fast", members);
        Simulator sim = Simulator.getInstance();
        sim.simulate(1);
        sim.shutdown();
        assertEquals("1", client.responses.getLast().query("value"));
    }
}
