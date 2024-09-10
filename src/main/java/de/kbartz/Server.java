package de.kbartz;

import org.oxoo2a.sim4da.Message;
import org.oxoo2a.sim4da.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.*;

public class Server extends Node {

    static int voteTimeoutLowerLimit = 150;
    static int voteTimeoutUpperLimit = 300;
    static final int heartbeatInterval = 30;


    // persistent state on all servers
//    TODO: maybe make it really persistent?
    private String id;
    private final ArrayList<String> cluster = new ArrayList<>();
    private int currentTerm = 0;
    String votedFor = null;
    private final ArrayList<LogEntry> log = new ArrayList<>();
    private final HashMap<String, String> stateMachine = new HashMap<>();

    // volatile state on all servers
    private int commitIndex = 0;
    private int lastApplied = 0;
    private ServerType serverType = ServerType.FOLLOWER;
    private final int voteTimeout;
    private long lastElectionTime = System.currentTimeMillis();
    private String leaderId;
    private boolean inTestingMode = false;

    // volatile state on leaders, to be reinitialized after election
    private final HashMap<String, Integer> nextIndex = new HashMap<>();
    private final HashMap<String, Integer> matchIndex = new HashMap<>();
    private Thread heartbeatThread;

    public Server(String name, ArrayList<String> cluster) {
        super(name);
        this.id = name;
        Random r = new Random();
        voteTimeout = r.nextInt(voteTimeoutUpperLimit - voteTimeoutLowerLimit + 1) + voteTimeoutLowerLimit;
        setCluster(cluster);
    }

    //    for testing purposes
    public Server(String name, int voteTimeout, ArrayList<String> cluster, boolean testMode) {
        super(name);
        this.id = name;
        this.voteTimeout = voteTimeout;
        setCluster(cluster);
        this.inTestingMode = testMode;
    }

    private void setCluster(ArrayList<String> cluster) {
        this.cluster.addAll(cluster);
        this.cluster.removeIf(member -> member.equals(this.id));
    }

    public AppendEntriesResult appendEntries(int term, String leaderId, int prevLogIndex, int prevLogTerm, ArrayList<LogEntry> entries, int leaderCommit) {
        AppendEntriesResult result = new AppendEntriesResult();
        result.setTerm(currentTerm);
//        If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
        if (term > currentTerm) {
            currentTerm = term;
            serverType = ServerType.FOLLOWER;
            if (heartbeatThread != null) {
                heartbeatThread.interrupt();
                heartbeatThread = null;
            }
        }
        //        only followers respond to rpcs
        if (serverType != ServerType.FOLLOWER) {
            result.setSuccess(false);
            return result;
        }
        if (term < currentTerm) {
            result.setSuccess(false);
            return result;
        }
//        log does not contain entry at prevLogIndex whose term matches prevLogTerm
        if (log.size() <= prevLogIndex || log.get(prevLogIndex).getTerm() != prevLogTerm) {
            result.setSuccess(false);
            return result;
        }
//        from here on downwards the rpc is accepted
        this.leaderId = leaderId;
//        existing entry conflicts with new entry
        int i = 0;
        boolean conflict = false;
        for (; i < entries.size(); i++) {
            LogEntry entry = entries.get(i);
            if (entry.getTerm() != term) {
                conflict = true;
                break;
            }
        }
        if (conflict) {
            int logIndex = prevLogIndex + 1 + i;
            if (log.size() > logIndex) {
                log.subList(logIndex, log.size()).clear();
            }
        }
        log.addAll(entries);
        if (leaderCommit > commitIndex) {
            commitIndex = Math.min(leaderCommit, log.size() - 1);
        }
//        If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
        if (commitIndex > lastApplied) {
//            TODO: does it make sense to increment it only by 1?
            lastApplied++;
//            commit log[lastApplied] to state machine
            LogEntry entry = log.get(lastApplied);
            stateMachine.put(entry.getKey(), entry.getValue());
        }
        result.setSuccess(true);
        result.setTerm(currentTerm);
        return result;
    }

    public RequestVoteResult requestVote(int term, String candidateId, int lastLogIndex, int lastLogTerm) {
        RequestVoteResult result = new RequestVoteResult();
        result.setTerm(currentTerm);
        //        only followers respond to rpcs
        if (serverType != ServerType.FOLLOWER) {
            result.setVoteGranted(false);
            return result;
        }
        if (term < currentTerm) {
            result.setVoteGranted(false);
            return result;
        }
        if (votedFor == null || votedFor.equals(candidateId)) {
            result.setVoteGranted(true);
            votedFor = candidateId;
            return result;
        }
        result.setVoteGranted(false);
        return result;
    }

    public void startElection() {
        serverType = ServerType.CANDIDATE;
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
            heartbeatThread = null;
        }
        currentTerm++;
        votedFor = id;
//        reset election timer
        lastElectionTime = System.nanoTime();
        Message msg = new Message();
        msg.addHeader("type", "requestVote");
        msg.add("term", currentTerm);
        msg.add("candidateId", id);
        msg.add("lastLogIndex", log.size() - 1);
        msg.add("lastLogTerm", currentTerm);
        for (String member : cluster)
            sendBlindly(msg, member);
        Future<Boolean> future = CompletableFuture.supplyAsync(() -> {
            int votes = 0;
//            majority of servers
            while (votes < Math.ceil(cluster.size() / 2.)) {
                Message m = receive();
//                If the leader’s term (included in its RPC) is at least as large as the candidate’s current term, then the candidate recognizes the leader as legitimate
                if (m.queryHeader("type").equals("appendEntries")) {
                    if (Integer.parseInt(m.query("term")) >= currentTerm) {
                        serverType = ServerType.FOLLOWER;
                        return false;
                    } else continue;
                } else if (!m.queryHeader("type").equals("requestVoteResponse")) continue;
                if (Integer.parseInt(m.query("voteGranted")) == 1)
                    votes++;
            }
//            received votes from a majority of servers
            serverType = ServerType.LEADER;
//            update leader state variables
            for (String member : cluster) {
//            initialized to leader last log index + 1
                nextIndex.put(member, log.size());
                matchIndex.put(member, 0);
            }
            heartbeatThread = new Thread(heartbeatRunnable);
            heartbeatThread.start();
            return true;
        });
        try {
            boolean won = future.get(voteTimeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
//            If election timeout elapses: start new election
            future.cancel(true);
            startElection();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void sendHeartbeats() {
        Message hb = new Message();
        hb.addHeader("type", "appendEntries");
        hb.add("term", currentTerm);
        hb.add("leaderId", id);
        hb.add("leaderCommit", commitIndex);
        hb.add("prevLogTerm", currentTerm);
        for (String member : cluster) {
            hb.add("prevLogIndex", nextIndex.get(member) - 1);
            sendBlindly(hb, member);
        }
    }

    //    for testing purposes
    public void voteFor(int term, String id) {
        voteFor(term, id, 1, 1);
    }

    private void voteFor(int term, String id, int lastLogIndex, int lastLogTerm) {
        RequestVoteResult res = requestVote(term, id, lastLogIndex, lastLogTerm);
        Message response = new Message();
        response.addHeader("type", "requestVoteResponse");
        response.add("term", res.getTerm());
        response.add("voteGranted", res.isVoteGranted() ? 1 : 0);
        sendBlindly(response, id);
    }

    private final Runnable heartbeatRunnable = new Runnable() {
        @Override
        public void run() {
            while (!Thread.interrupted()) {
                sendHeartbeats();
                try {
                    Thread.sleep(heartbeatInterval);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    };

    @Override
    protected void engage() {
        while (true) {
            if (serverType == ServerType.LEADER) {
                Message m = receive();
                switch (m.queryHeader("type")) {
                    case "appendEntriesResponse": {

                        break;
                    }
                    case "requestVoteResponse": {

                        break;
                    }
                }
            } else {
                Future<Message> future = CompletableFuture.supplyAsync(this::receive);
                try {
                    Message m = future.get(voteTimeout, TimeUnit.MILLISECONDS);
                    if (inTestingMode) continue;
                    switch (m.queryHeader("type")) {
                        case "requestVote": {
                            int term = Integer.parseInt(m.query("term"));
                            String candidateId = m.query("candidateId");
                            int lastLogIndex = Integer.parseInt(m.query("lastLogIndex"));
                            int lastLogTerm = Integer.parseInt(m.query("lastLogTerm"));
                            voteFor(term, candidateId, lastLogIndex, lastLogTerm);
                            break;
                        }
                        case "appendEntries": {
                            int term = Integer.parseInt(m.query("term"));
                            String leaderId = m.query("leaderId");
                            int prevLogIndex = Integer.parseInt(m.query("prevLogIndex"));
                            int prevLogTerm = Integer.parseInt(m.query("prevLogTerm"));
                            int leaderCommit = Integer.parseInt(m.query("leaderCommit"));
                            String key = m.query("key");
                            String value = m.query("value");
                            if (key == null || value == null) {
                                continue;
                            }
                            LogEntry entry = new LogEntry(term, key, value);
                            ArrayList<LogEntry> entries = new ArrayList<>();
                            entries.add(entry);
                            AppendEntriesResult res = appendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
                            Message response = new Message();
                            response.addHeader("type", "appendEntriesResponse");
                            response.add("term", res.getTerm());
                            response.add("success", res.isSuccess() ? 1 : 0);
                            sendBlindly(response, m.queryHeader("sender"));
                            break;
                        }
                    }
                } catch (TimeoutException e) {
                    future.cancel(true);
                    long timePassed = System.currentTimeMillis() - lastElectionTime;
                    if (timePassed > voteTimeout)
                        startElection();
                } catch (ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public ServerType getServerType() {
        return serverType;
    }

    public int getCurrentTerm() {
        return currentTerm;
    }
}

enum ServerType {
    LEADER,
    FOLLOWER,
    CANDIDATE
}
