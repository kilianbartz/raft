package de.kbartz;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.oxoo2a.sim4da.Message;
import org.oxoo2a.sim4da.Node;

import java.util.List;

public class TestClient extends Node {

    private final String recipient;
    private final List<LogEntry> log;
    ObjectMapper mapper = new ObjectMapper();

    public TestClient(String name, List<LogEntry> log, String recipient) {
        super(name);
        this.log = log;
        this.recipient = recipient;
    }

    @Override
    protected void engage() {
        try {
            Thread.sleep(500);
            Message msg = new Message();
            msg.addHeader("type", "clientPut");
            msg.add("entries", mapper.writeValueAsString(log));
            sendBlindly(msg, recipient);
        } catch (InterruptedException | JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
