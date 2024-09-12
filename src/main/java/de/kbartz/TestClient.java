package de.kbartz;

import org.oxoo2a.sim4da.Message;
import org.oxoo2a.sim4da.Node;

import java.util.List;

public class TestClient extends Node {

    private final String recipient;
    private final List<LogEntry> log;

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
            msg.addHeader("type", "clientAppend");
            for (LogEntry entry : log) {
                msg.add("key", entry.getKey());
                msg.add("value", entry.getValue());
                sendBlindly(msg, recipient);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
