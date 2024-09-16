package de.kbartz;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.oxoo2a.sim4da.Message;

import java.util.List;

public class TestClientMultiple extends TestClient {

    private final List<Pair<List<LogEntry>, String>> logs;
    private final Object lock = new Object();
    private final boolean waitForAnswer;

    public TestClientMultiple(String id, List<Pair<List<LogEntry>, String>> logs, String recipient, List<String> cluster, boolean waitForAnswer) {
        super(id, logs.getFirst().first, recipient, cluster);
        this.logs = logs;
        this.waitForAnswer = waitForAnswer;
    }

    private final Runnable checkForMessages = () -> {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < TIMEOUT) {
            Message m = receive();
            System.out.println("Client " + this.id + " received " + m);
            if (m.query("success").equals("1"))
                successes++;
            else {
                failures++;
            }
            synchronized (lock) {
                lock.notify();
            }
        }
    };

    @Override
    protected void engage() {
        try {
            Thread.sleep(INITIAL_WAIT_TIME);
            new Thread(checkForMessages).start();
            synchronized (lock) {
                int counter = 0;
                for (Pair<List<LogEntry>, String> log : logs) {
                    Message msg = new Message();
                    msg.addHeader("type", "clientPut");
                    msg.add("entries", mapper.writeValueAsString(log.first));
                    msg.add("uuid", log.second);
                    sendBlindly(msg, recipient);
                    System.out.println(this.id + " send request " + counter++);
                    if (waitForAnswer) {
                        lock.wait();
                    }
                }
            }
        } catch (InterruptedException | JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
