package de.kbartz;

import org.oxoo2a.sim4da.Message;
import org.oxoo2a.sim4da.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.*;

public class Server extends Node {

    static int voteTimeoutLowerLimit = 10;
    static int voteTimeoutUpperLimit = 100;

    // persistent state on all servers
//    TODO: maybe make it really persistent?
    private String id;
    private final ArrayList<String> members = new ArrayList<>();
    private int currentTerm = 0;
    String votedFor = null;
    private final ArrayList<LogEntry> log = new ArrayList<>();
    private final HashMap<String, String> stateMachine = new HashMap<>();

    // volatile state on all servers
    private int commitIndex = 0;
    private int lastApplied = 0;
    private ServerType serverType = ServerType.FOLLOWER;
    private final int voteTimeout;

    // volatile state on leaders, to be reinitialized after election
    private final HashMap<String, Integer> nextIndex = new HashMap<>();
    private final HashMap<String, Integer> matchIndex = new HashMap<>();

    public Server(String name) {
        super(name);
        Random r = new Random();
        voteTimeout = r.nextInt(voteTimeoutUpperLimit - voteTimeoutLowerLimit + 1) + voteTimeoutLowerLimit;
    }

    public AppendEntriesResult appendEntries(int term, int leaderId, int prevLogIndex, int prevLogTerm, ArrayList<LogEntry> entries, int leaderCommit) {
        AppendEntriesResult result = new AppendEntriesResult();
        result.setTerm(currentTerm);
//        If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
        if (term > currentTerm){
            currentTerm = term;
            serverType = ServerType.FOLLOWER;
        }
        //        only followers respond to rpcs
        if (serverType != ServerType.FOLLOWER){
            result.setSuccess(false);
            return result;
        }
        if (term < currentTerm) {
            result.setSuccess(false);
            return result;
        }
//        log does not contain entry at prevLogIndex whose term matches prevLogTerm
        if (log.size() <= prevLogIndex || log.get(prevLogIndex).getTerm() != prevLogTerm){
            result.setSuccess(false);
            return result;
        }
//        existing entry conflicts with new entry
        int i = 0;
        boolean conflict = false;
        for(; i < entries.size(); i++){
            LogEntry entry = entries.get(i);
            if (entry.getTerm() != term){
                conflict = true;
                break;
            }
        }
        if (conflict){
            int logIndex = prevLogIndex + 1 + i;
            if (log.size() > logIndex) {
                log.subList(logIndex, log.size()).clear();
            }
        }
        log.addAll(entries);
        if (leaderCommit > commitIndex){
            commitIndex = Math.min(leaderCommit, log.size()-1);
        }
//        If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
        if (commitIndex > lastApplied){
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
        if (serverType != ServerType.FOLLOWER){
            result.setVoteGranted(false);
            return result;
        }
        if (term < currentTerm) {
            result.setVoteGranted(false);
            return result;
        }
        if (votedFor == null || votedFor.equals(candidateId)){
            result.setVoteGranted(true);
            votedFor = candidateId;
            return result;
        }
        result.setVoteGranted(false);
        return result;
    }

    public void startElection(){
        serverType = ServerType.CANDIDATE;
        currentTerm++;
        votedFor = id;
//        TODO: reset election timer
        Message msg = new Message();
        msg.addHeader("type", "requestVote");
        for (String member: members)
            sendBlindly(msg, member);
        Future<Boolean> future = CompletableFuture.supplyAsync(() -> {
            int votes = 0;
            while (votes < members.size() / 2){
                Message m = receive();
                if (!m.queryHeader("type").equals("requestVoteResponse"))continue;
                if (Integer.parseInt(m.query("voteGranted")) == 1)
                    votes++;
            }
//            received votes from a majority of servers
            serverType = ServerType.LEADER;
//            update leader state variables
            for(String member: members){
//            initialized to leader last log index + 1
                nextIndex.put(member, log.size());
                matchIndex.put(member, 0);
            }
            return false;
        });
        try {
            future.get(voteTimeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
//            If election timeout elapses: start new election
            future.cancel(true);
            startElection();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void sendHeartbeats(){
        Message hb = new Message();
        hb.addHeader("type", "appendEntries");
        hb.add("term", currentTerm);
        hb.add("leaderId", id);
        hb.add("leaderCommit", commitIndex);
        hb.add("prevLogTerm", currentTerm);
        for (String member: members){
            hb.add("prevLogIndex", nextIndex.get(member)-1);
            sendBlindly(hb, member);
        }
    }

    @Override
    protected void engage(){
        while (true){
            if (serverType == ServerType.LEADER){
                sendHeartbeats();
                try {
                    Thread.sleep(30);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                Future<Message> future = CompletableFuture.supplyAsync(this::receive);
                try {
                    Message m = future.get(voteTimeout, TimeUnit.MILLISECONDS);
                    switch (m.queryHeader("type")){
                        case "requestVote": {
                            int term = Integer.parseInt(m.query("term"));
                            String candidateId = m.query("candidateId");
                            int lastLogIndex = Integer.parseInt(m.query("lastLogIndex"));
                            int lastLogTerm = Integer.parseInt(m.query("lastLogTerm"));
                            RequestVoteResult res = requestVote(term, candidateId, lastLogIndex, lastLogTerm);
                            Message response = new Message();
                            response.addHeader("type", "requestVoteResponse");
                            response.add("term", res.getTerm());
                            response.add("voteGranted", res.isVoteGranted() ? 1 : 0);
                            sendBlindly(response, m.queryHeader("sender"));
                            break;
                        }
                        case "appendEntries": {
                            int term = Integer.parseInt(m.query("term"));
                            int leaderId = Integer.parseInt(m.query("leaderId"));
                            int prevLogIndex = Integer.parseInt(m.query("prevLogIndex"));
                            int prevLogTerm = Integer.parseInt(m.query("prevLogTerm"));
                            int leaderCommit = Integer.parseInt(m.query("leaderCommit"));
                            String key = m.query("key");
                            String value = m.query("value");
                            if (key == null || value == null){
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
                        case "appendEntriesResponse": {

                            break;
                        }
                    }
                } catch (TimeoutException e){
                    startElection();
                    future.cancel(true);
                } catch (ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

enum ServerType {
    LEADER,
    FOLLOWER,
    CANDIDATE
}
