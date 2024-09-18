package de.kbartz;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.oxoo2a.sim4da.Message;
import org.oxoo2a.sim4da.Simulator;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestMembershipChanges {

    //    Achtung: die Tests laufen nur einzeln, nicht hintereinander durch. Das liegt wahrscheinlich an der simulator Instanz, die wiederverwendet wird
    @Test
    void testFollowerDisconnects() throws JsonProcessingException {
        ArrayList<String> members = new ArrayList<>();
        members.add("fast");
        members.add("slow");
        members.add("slow2");
        Server s1 = new Server("fast", 50, members);
        Server s3 = new Server("slow", 150, members);
        Server s4 = new Server("slow2", 150, members);

        List<String> newMembers = members.stream().filter(member -> !member.equals("slow2")).toList();
        Message changeMembership = new Message();
        changeMembership.addHeader("type", "configChange");
        ObjectMapper mapper = new ObjectMapper();
        changeMembership.add("cluster", mapper.writeValueAsString(newMembers));

        Message[] messages = new Message[]{
                TestClientAppends.convertToMessage(new LogEntry[]{new LogEntry("x", "1")}),
                changeMembership
        };
        TestClient client = new TestClient("client", messages, "fast", members);
        Simulator sim = Simulator.getInstance();
        sim.simulate(1);
        sim.shutdown();
        assertEquals(ServerType.LEADER, s1.getServerType());
        assertEquals(ServerType.FOLLOWER, s3.getServerType());
        assertEquals(ServerType.CANDIDATE, s4.getServerType());
        assertEquals(MembershipchangeState.REGULAR, s1.getMembershipChangeState());
        assertEquals(1, s1.getMembersToReplicateTo().size());
    }

    @Test
    void testNewServer() throws JsonProcessingException {
        ArrayList<String> members = new ArrayList<>();
        members.add("fast");
        members.add("slow");
        members.add("slow2");
        Server s1 = new Server("fast", 50, members);
        s1.setCurrentTerm(30);
        Server s3 = new Server("slow", 150, members);
        Server s4 = new Server("slow2", 150, members);
        Server s5 = new Server("later", 200, members);

        List<String> newMembers = new ArrayList<>(members);
        newMembers.add("later");
        Message changeMembership = new Message();
        changeMembership.addHeader("type", "configChange");
        ObjectMapper mapper = new ObjectMapper();
        changeMembership.add("cluster", mapper.writeValueAsString(newMembers));

        Message[] messages = new Message[]{
                TestClientAppends.convertToMessage(new LogEntry[]{new LogEntry("x", "1")}),
                changeMembership
        };
        TestClient client = new TestClient("client", messages, "fast", members);
        Simulator sim = Simulator.getInstance();
        sim.simulate(2);
        sim.shutdown();
        assertEquals(ServerType.LEADER, s1.getServerType());
        assertEquals(ServerType.FOLLOWER, s3.getServerType());
        assertEquals(ServerType.FOLLOWER, s4.getServerType());
        assertEquals(MembershipchangeState.REGULAR, s1.getMembershipChangeState());
        assertEquals(s1.getLog().size(), s5.getLog().size());
    }
}
