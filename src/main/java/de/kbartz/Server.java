package de.kbartz;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.oxoo2a.sim4da.Message;
import org.oxoo2a.sim4da.Node;

import java.util.*;

@SuppressWarnings({"BusyWait", "InfiniteLoopStatement"})
public class Server extends Node {

//    TODO: maybe use Virtual Threads https://docs.oracle.com/en/java/javase/21/core/virtual-threads.html#GUID-A0E4C745-6BC3-4DAE-87ED-E4A094D20A38

    static int voteTimeoutLowerLimit = 150;
    static int voteTimeoutUpperLimit = 300;
    static final int heartbeatInterval = 30;

    ObjectMapper mapper = new ObjectMapper();


    // persistent state on all servers
//    TODO: maybe make it really persistent?
    private String id = "";
    private final ArrayList<String> cluster = new ArrayList<>();
    private int currentTerm = 0;
    String votedFor = null;
    private final ArrayList<LogEntry> log = new ArrayList<>();
    private final HashMap<String, String> stateMachine = new HashMap<>();

    // volatile state on all servers
    private int commitIndex = 0;
    private int lastApplied = 0;
    private ServerType serverType = ServerType.FOLLOWER;
    private int voteTimeout = 0;
    private String leaderId;
    private LinkedList<Message> queue = new LinkedList<>();
    private final Object lock = new Object();
    private final Object logLock = new Object();

    private int votesForMe = 0;
    private long lastRelevantTime = System.currentTimeMillis(); //only set when receiving valid appendEntries / granting vote

    // volatile state on leaders, to be reinitialized after election
    private final HashMap<String, Integer> nextIndex = new HashMap<>();
    private final HashMap<String, Integer> matchIndex = new HashMap<>();
    private final HashMap<String, Pair<Integer, Integer>> clientIndices = new HashMap<>();
    private Thread heartbeatThread;

    public Server(String name, ArrayList<String> cluster) {
        super(name);
        this.id = name;
        Random r = new Random();
        voteTimeout = r.nextInt(voteTimeoutUpperLimit - voteTimeoutLowerLimit + 1) + voteTimeoutLowerLimit;
        setCluster(cluster);
    }

    //    for testing purposes
    public Server(String name, int voteTimeout, ArrayList<String> cluster) {
        super(name);
        this.id = name;
        this.voteTimeout = voteTimeout;
        setCluster(cluster);
    }

    private void setCluster(ArrayList<String> cluster) {
        this.cluster.addAll(cluster);
        this.cluster.removeIf(member -> member.equals(this.id));
    }

    public AppendEntriesResult appendEntries(int term, String leaderId, int prevLogIndex, int prevLogTerm, LogEntry[] entries, int leaderCommit) {
        AppendEntriesResult result = new AppendEntriesResult();
        result.setTerm(currentTerm);
        if (term < currentTerm) {
            System.out.println(this.id + " rejected appendEntries cause of term");
            result.setSuccess(false);
            return result;
        }
//        log does not match leader's log: does not contain entry at prevLogIndex whose term matches prevLogTerm;
        if (prevLogIndex >= 0 && (log.size() <= prevLogIndex || log.get(prevLogIndex).getTerm() != prevLogTerm)) {
            result.setSuccess(false);
            return result;
        }
//        from here on downwards the rpc is accepted
        lastRelevantTime = System.currentTimeMillis();
        this.leaderId = leaderId;
//        existing entry conflicts with new entry
        int i = 0;
        boolean conflict = false;
        for (; i < entries.length; i++) {
            LogEntry entry = entries[i];
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
        log.addAll(List.of(entries));
        if (leaderCommit > commitIndex) {
            commit(commitIndex, leaderCommit);
            commitIndex = leaderCommit;
        }
        result.setSuccess(true);
        result.setTerm(currentTerm);
        return result;
    }

    public RequestVoteResult requestVote(int term, String candidateId, int lastLogIndex, int lastLogTerm) {
        RequestVoteResult result = new RequestVoteResult();
        result.setTerm(currentTerm);
        if (term < currentTerm) {
            System.out.println(this.id + " rejects because of term too small");
            result.setVoteGranted(false);
            return result;
        }
// additional election restrictions (5.4.1) to ensure that the candidate has all previously commited entries
        if ((!log.isEmpty() && log.getLast().getTerm() > lastLogTerm) || log.size() - 1 > lastLogIndex) {
            System.out.println(this.id + " rejects because of log not up to date");
            result.setVoteGranted(false);
            return result;
        }
        lastRelevantTime = System.currentTimeMillis();
        if (votedFor == null || votedFor.equals(candidateId)) {
            result.setVoteGranted(true);
            votedFor = candidateId;
            return result;
        }
        result.setTerm(term);
        result.setVoteGranted(false);
        return result;
    }

    public void election() {
        serverType = ServerType.CANDIDATE;
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
            heartbeatThread = null;
        }
        currentTerm++;
        System.out.println(this.id + " started election in term " + currentTerm + " (" + System.currentTimeMillis() + ")");
        votedFor = id;
        synchronized (lock) {
            votesForMe = 1;
        }
        Message msg = new Message();
        msg.addHeader("type", "requestVote");
        msg.add("term", currentTerm);
        msg.add("candidateId", id);
        msg.add("lastLogIndex", log.size() - 1);
        msg.add("lastLogTerm", log.isEmpty() ? currentTerm : log.getLast().getTerm());
        for (String member : cluster)
            sendBlindly(msg, member);
    }

    public void sendHeartBeat(String member) {
        Message hb = new Message();
        hb.addHeader("type", "appendEntries");
        hb.add("term", currentTerm);
        hb.add("leaderId", id);
        hb.add("leaderCommit", commitIndex);
        hb.add("entries", "[]");
        int prevLogIndex = nextIndex.get(member) - 1;
        int prevLogTerm = 0;
        if (prevLogIndex >= 0) {
            prevLogTerm = log.get(prevLogIndex).getTerm();
        }
        hb.add("prevLogIndex", prevLogIndex);
        hb.add("prevLogTerm", prevLogTerm);
        if (nextIndex.get(member) < log.size()) {
//                member is missing entries
            List<LogEntry> toSend = log.subList(nextIndex.get(member), log.size());
            System.out.println(member + " is behind by: " + toSend.size());
            try {
                String entries = mapper.writeValueAsString(toSend);
                hb.add("entries", entries);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        sendBlindly(hb, member);
    }

    private final Runnable heartbeatRunnable = () -> {
        while (!Thread.interrupted()) {
            System.out.println("Sending heartbeats at " + System.currentTimeMillis());
            for (String member : cluster) {
                sendHeartBeat(member);
            }
            try {
                Thread.sleep(heartbeatInterval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    };

    private void wonElection() {
        System.out.println(this.id + " won the election with " + this.votesForMe + " votes.");
        serverType = ServerType.LEADER;
        heartbeatThread = new Thread(heartbeatRunnable);
        heartbeatThread.start();
        for (String member : cluster) {
            nextIndex.put(member, log.size());
            matchIndex.put(member, 0);
        }
    }

    private final Runnable electionTimeout = () -> {
        while (true) {
            try {
                Thread.sleep(this.voteTimeout);
                if (serverType == ServerType.LEADER)
                    continue;
                synchronized (lock) {
                    if (System.currentTimeMillis() - lastRelevantTime > voteTimeout)
                        election();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    };

    private void processAppendEntries(Message msg) {
        int term = Integer.parseInt(msg.query("term"));
        String leaderId = msg.query("leaderId");
        int prevLogIndex = Integer.parseInt(msg.query("prevLogIndex"));
        int prevLogTerm = Integer.parseInt(msg.query("prevLogTerm"));
        int leaderCommit = Integer.parseInt(msg.query("leaderCommit"));
        try {
            LogEntry[] entries = mapper.readValue(msg.query("entries"), LogEntry[].class);
            AppendEntriesResult res = appendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
            Message response = new Message();
            response.addHeader("type", "appendEntriesResponse");
            response.add("term", res.getTerm());
            response.add("success", res.isSuccess() ? 1 : 0);
            if (res.isSuccess())
                response.add("matchIndex", log.size() - 1);
            sendBlindly(response, msg.queryHeader("sender"));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private void becomeFollower() {
        serverType = ServerType.FOLLOWER;
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
            heartbeatThread = null;
        }
    }

    private void processMessage(Message msg) {
        System.out.println(msg);
        int term = Integer.parseInt(Objects.requireNonNullElse(msg.query("term"), "0"));
        //        If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
        if (term > currentTerm) {
            System.out.println(this.id + " updated term and becomes follower");
            currentTerm = term;
//            not voted for anyone in new term
            votedFor = null;
            becomeFollower();
        }
        switch (serverType) {
            case CANDIDATE: {
                switch (msg.queryHeader("type")) {
                    case "appendEntries": {
//                    While waiting for votes, a candidate may receive an AppendEntries RPC from another server claiming to be leader. If the leader’s term (included in its RPC) is at least as large as the candidate’s current term, then the candidate recognizes the leader as legitimate and returns to follower state
                        if (term >= currentTerm) {
                            serverType = ServerType.FOLLOWER;
                            System.out.println("Another server was quicker, reverting to follower state");
                            processAppendEntries(msg);
                        }
                        break;
                    }
                    case "requestVoteResponse": {
                        boolean voteGranted = msg.query("voteGranted").equals("1");
                        System.out.println(this.id + " received reponse from " + msg.queryHeader("sender") + " for term " + term + ": " + voteGranted + " (" + System.currentTimeMillis() + ")");
                        if (voteGranted) {
                            votesForMe++;
                        }
                        if (votesForMe > Math.ceil((cluster.size() + 1) / 2.))
                            wonElection();
                    }
                    default:
                        return;
                }
                break;
            }
            case FOLLOWER: {
                switch (msg.queryHeader("type")) {
                    case "requestVote": {
                        String candidateId = msg.query("candidateId");
                        int lastLogIndex = Integer.parseInt(msg.query("lastLogIndex"));
                        int lastLogTerm = Integer.parseInt(msg.query("lastLogTerm"));
                        RequestVoteResult result = requestVote(term, candidateId, lastLogIndex, lastLogTerm);
                        Message reply = new Message();
                        reply.addHeader("type", "requestVoteResponse");
                        reply.add("term", term);
                        reply.add("voteGranted", result.isVoteGranted() ? "1" : "0");
                        sendBlindly(reply, candidateId);
                        break;
                    }
                    case "appendEntries": {
                        processAppendEntries(msg);
                        break;
                    }
                    default:
                        return;
                }
                break;
            }
            case LEADER: {
                switch (msg.queryHeader("type")) {
                    case "clientPut": {
                        try {
                            List<LogEntry> entries = Arrays.asList(mapper.readValue(msg.query("entries"), LogEntry[].class));
//                            set correct terms
                            for (LogEntry entry : entries) {
                                entry.setTerm(currentTerm);
                            }
                            synchronized (logLock) {
                                int startIndex = log.size();
                                int endIndex = startIndex + entries.size() - 1;
                                log.addAll(entries);
                                clientIndices.put(msg.queryHeader("sender"), new Pair<>(startIndex, endIndex));
                            }
                            System.out.println("leader received " + entries.size() + " entries from " + msg.queryHeader("sender"));
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    }
                    case "appendEntriesResponse": {
                        String sender = msg.queryHeader("sender");
                        if (!msg.query("success").equals("1")) {
//                            If AppendEntry fails: decrement nextIndex and retry
                            System.out.println("appendEntriesResponse failed. Retrying");
                            nextIndex.put(sender, nextIndex.get(sender) - 1);
                            sendHeartBeat(sender);
                            return;
                        }
//                        successful
                        int _matchIndex = Integer.parseInt(msg.query("matchIndex"));
                        matchIndex.put(sender, _matchIndex);
                        nextIndex.put(sender, _matchIndex + 1);
                        updateLeaderCommitIndex();
                        break;
                    }
                    default:
                        return;
                }
                break;
            }
        }
    }

    private final Runnable processMessages = () -> {
        while (true) {
            synchronized (lock) {
                try {
                    while (queue.peek() == null)
                        lock.wait();
                    while (queue.peek() != null) {
                        Message m = queue.poll();
                        processMessage(m);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    };


    //    for testing purposes
    public void sendVote(int term, String id) {
        sendVote(term, id, 1, 1);
    }

    private void sendVote(int term, String id, int lastLogIndex, int lastLogTerm) {
        RequestVoteResult res = requestVote(term, id, lastLogIndex, lastLogTerm);
        Message response = new Message();
        response.addHeader("type", "requestVoteResponse");
        response.add("term", res.getTerm());
        response.add("voteGranted", res.isVoteGranted() ? 1 : 0);
        System.out.println(this.id + " replied to vote with " + res.isVoteGranted() + " at " + System.currentTimeMillis());
        sendBlindly(response, id);
    }

    public void commit(int fromIndex, int toIndex) {
        System.out.println(this.id + " commited from " + fromIndex + " to " + toIndex);
        for (int i = fromIndex; i <= toIndex; i++) {
            LogEntry e = log.get(i);
            stateMachine.put(e.getKey(), e.getValue());
        }
    }

    public boolean updateLeaderCommitIndex() {
        for (int nextCommitIndex = log.size() - 1; nextCommitIndex > commitIndex; nextCommitIndex--) {
            int counter = 1;
            for (String member : cluster) {
                if (matchIndex.get(member) >= nextCommitIndex) {
                    counter++;
                }
            }
            if (counter > Math.ceil((cluster.size() + 1) / 2.)) {
                commit(commitIndex, nextCommitIndex);
                commitIndex = nextCommitIndex;
                System.out.println("leader: new commitIndex" + commitIndex);
//                responses to clients
//                TODO: no synchronization needed?
                ArrayList<String> safeToRemove = new ArrayList<>();
                Message successMessage = new Message();
                successMessage.addHeader("type", "clientPutResponse");
                successMessage.add("success", 1);
                for (Map.Entry<String, Pair<Integer, Integer>> entry : clientIndices.entrySet()) {
                    Pair<Integer, Integer> indices = entry.getValue();
                    if (indices.first <= commitIndex) {
                        try {
                            int endIndex = Math.min(commitIndex, indices.second);
                            String client = entry.getKey();
                            successMessage.add("entries", mapper.writeValueAsString(log.subList(indices.first, endIndex)));
                            sendBlindly(successMessage, client);
                            if (endIndex == indices.second) {
                                safeToRemove.add(client);
                            }
//                            else: only partially commited, another message will be sent when commited fully
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                for (String client : safeToRemove) {
                    clientIndices.remove(client);
                }
                return true;
            }
        }
        return false;
    }

    @Override
    protected void engage() {
        new Thread(electionTimeout).start();
        new Thread(processMessages).start();
        while (true) {
            Message m = receive();
            synchronized (lock) {
                queue.offer(m);
                lock.notify();
            }
        }
    }

    public ServerType getServerType() {
        return serverType;
    }

    public ArrayList<LogEntry> getLog() {
        return log;
    }

    public HashMap<String, String> getStateMachine() {
        return stateMachine;
    }

    public int getCommitIndex() {
        return commitIndex;
    }
}
