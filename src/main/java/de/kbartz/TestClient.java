package de.kbartz;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.oxoo2a.sim4da.Message;
import org.oxoo2a.sim4da.Node;

import java.util.ArrayList;
import java.util.List;

public class TestClient extends Node {

    protected final String recipient;
    private final Message[] messages;
    private final List<String> cluster;
    ObjectMapper mapper = new ObjectMapper();
    public int successes = 0;
    public int failures = 0;
    protected final String id;
    protected int initialWaitTime = 500;
    public List<Message> responses = new ArrayList<>();

    public TestClient(String id, Message[] messages, String recipient, List<String> cluster) {
        super(id);
        this.messages = messages;
        this.recipient = recipient;
        this.id = id;
        this.cluster = cluster;
    }

    public TestClient(String id, Message[] messages, String recipient, List<String> cluster, int initialWaitTime) {
        super(id);
        this.messages = messages;
        this.recipient = recipient;
        this.id = id;
        this.cluster = cluster;
        this.initialWaitTime = initialWaitTime;
    }


    @Override
    protected void engage() {
        try {
            Thread.sleep(initialWaitTime);
            for (Message message : messages) {
                sendBlindly(message, recipient);
//                wait for response before sending next message
                Message m = receive();
                responses.add(m);
                System.out.println("Client " + this.id + " received " + m);
                if (m.query("success").equals("1"))
                    successes++;
                else {
                    failures++;
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
