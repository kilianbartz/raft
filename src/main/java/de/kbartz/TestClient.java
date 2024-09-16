package de.kbartz;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.oxoo2a.sim4da.Message;
import org.oxoo2a.sim4da.Node;

import java.util.List;
import java.util.Random;
import java.util.UUID;

public class TestClient extends Node {

    protected final String recipient;
    private final List<LogEntry> log;
    private final List<String> cluster;
    ObjectMapper mapper = new ObjectMapper();
    public int successes = 0;
    public int failures = 0;
    protected final String id;
    protected final static int INITIAL_WAIT_TIME = 500;
    protected final static int TIMEOUT = 700;
    private final boolean waitForSuccess = true;
    private final Object lock = new Object();

    public TestClient(String id, List<LogEntry> log, String recipient, List<String> cluster) {
        super(id);
        this.log = log;
        this.recipient = recipient;
        this.id = id;
        this.cluster = cluster;
    }

    @Override
    protected void engage() {
        try {
            Thread.sleep(INITIAL_WAIT_TIME);
            Message msg = new Message();
            msg.addHeader("type", "clientPut");
            msg.add("entries", mapper.writeValueAsString(log));
            msg.add("uuid", UUID.randomUUID().toString());
            sendBlindly(msg, recipient);
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start < TIMEOUT) {
                Message m = receive();
                System.out.println("Client " + this.id + " received " + m);
                if (m.query("success").equals("1"))
                    successes++;
                else {
                    String leaderId = m.query("leaderId");
                    sendBlindly(msg, leaderId);
                    failures++;
                }
            }
            //timeout for message back
            if (successes == 0 && failures == 0) {
                Random randomizer = new Random();
                String random = cluster.get(randomizer.nextInt(cluster.size()));
                sendBlindly(msg, random);
            }
        } catch (InterruptedException | JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
