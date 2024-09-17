package de.kbartz;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.oxoo2a.sim4da.Message;
import org.oxoo2a.sim4da.Node;

import java.util.*;
import java.util.stream.Stream;

@SuppressWarnings({"BusyWait", "InfiniteLoopStatement"})
public class Server extends Node {

//    TODO: maybe use Virtual Threads https://docs.oracle.com/en/java/javase/21/core/virtual-threads.html#GUID-A0E4C745-6BC3-4DAE-87ED-E4A094D20A38

    static int voteTimeoutLowerLimit = 30;
    static int voteTimeoutUpperLimit = 300;
    static final int heartbeatInterval = 30;

    ObjectMapper mapper = new ObjectMapper();


    // persistent state on all servers
    private String id = "";
    private List<String> cluster = new ArrayList<>();
    private List<String> clusterOld = new ArrayList<>();
    private List<String> clusterNew = new ArrayList<>();
    private List<String> membersToReplicateTo = cluster;
    private List<String> nonVotingMembers = new ArrayList<>();
    private int cOldNewIndex = -1;
    private int cNewIndex = -1;

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
    private final LinkedList<Message> queue = new LinkedList<>();
    private final Object messageLock = new Object();
    private final Object logLock = new Object();

    private int votesForMe = 0;
    private int votesForMeOld = 0;
    private int votesForMeNew = 0;
    private long lastRelevantTime = System.currentTimeMillis(); //only set when receiving valid appendEntries / granting vote

    // volatile state on leaders, to be reinitialized after election
    private final HashMap<String, Integer> nextIndex = new HashMap<>();
    private final HashMap<String, Integer> matchIndex = new HashMap<>();
    private final HashMap<String, Pair<Integer, Integer>> clientIndices = new HashMap<>();
    private final HashMap<String, Pair<String, Boolean>> lastUuids = new HashMap<>();
    private Thread heartbeatThread;

    private final LinkedList<GetRequest> getQueue = new LinkedList<>();
    private final Object getQueueLock = new Object();
    private MembershipchangeState membershipChangeState = MembershipchangeState.REGULAR;

    public Server(String name, ArrayList<String> cluster) {
        super(name);
        this.id = name;
        Random r = new Random();
        voteTimeout = r.nextInt(voteTimeoutUpperLimit - voteTimeoutLowerLimit + 1) + voteTimeoutLowerLimit;
        setCluster(cluster, this.cluster);
    }

    //    for testing purposes
    public Server(String name, int voteTimeout, ArrayList<String> cluster) {
        super(name);
        this.id = name;
        this.voteTimeout = voteTimeout;
        setCluster(cluster, this.cluster);
    }

    private void setCluster(List<String> cluster, List<String> dest) {
        dest.addAll(cluster);
        dest.removeIf(member -> member.equals(this.id));
    }

    public AppendEntriesResult appendEntries(int term, String leaderId, int prevLogIndex, int prevLogTerm, LogEntry[] entries, int leaderCommit) {
        AppendEntriesResult result = new AppendEntriesResult();
        result.setTerm(currentTerm);
        if (term < currentTerm) {
            System.out.println(this.id + " rejected appendEntries because of term");
            result.setSuccess(false);
            return result;
        }
//        log does not match leader's log: does not contain entry at prevLogIndex whose term matches prevLogTerm;
        if (prevLogIndex >= 0 && (log.size() <= prevLogIndex || log.get(prevLogIndex).getTerm() != prevLogTerm)) {
            System.out.println(this.id + " rejected appendEntries because of inconsistent log");
            result.setSuccess(false);
            return result;
        }
//        from here on downwards the rpc is accepted
        int insertIndex = prevLogIndex + 1;
        lastRelevantTime = System.currentTimeMillis();
        this.leaderId = leaderId;
//        own log might be too long (duplicate entries etc.). This also covers conflicting entries
        if (log.size() - 1 > prevLogIndex) {
            log.subList(insertIndex, log.size()).clear();
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
        long time = System.currentTimeMillis();
        if (time - lastRelevantTime < voteTimeoutLowerLimit) {
            System.out.println(this.id + " rejects because leader just contacted " + this.id + " " + (time - lastRelevantTime) + " ms ago");
            result.setVoteGranted(false);
            return result;
        }
        lastRelevantTime = time;
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
        synchronized (messageLock) {
            votesForMe = 1;
            votesForMeNew = 1;
            votesForMeOld = 1;
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
            for (String member : membersToReplicateTo) {
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
        clientIndices.clear();
        lastUuids.clear();
//        commit no-op at start of term to find out which entries are commited (§8)
        synchronized (logLock) {
            log.add(new LogEntry(currentTerm, "noop", ""));
        }
    }

    private final Runnable electionTimeout = () -> {
        while (true) {
            try {
                Thread.sleep(this.voteTimeout);
                if (serverType == ServerType.LEADER)
                    continue;
                synchronized (messageLock) {
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
            if (res.isSuccess()) {
                response.add("matchIndex", log.size() - 1);
                for (LogEntry entry : entries) {
                    // after server receives a new config, it is used for all future decisions (a server always uses the latest configuration in its log, regardless of whether the entry is committed)
                    if (entry.getKey().equals("config:cOldNew")) {
                        String[][] cOldNewSer = mapper.readValue(entry.getValue(), String[][].class);
                        clusterOld = Arrays.asList(cOldNewSer[0]);
                        clusterNew = Arrays.asList(cOldNewSer[1]);
                    }
                    if (entry.getKey().equals("config:cNew")) {
                        String[] members = mapper.readValue(entry.getValue(), String[].class);
                        cluster = Arrays.asList(members);
                        clusterOld = null;
                        clusterNew = null;
                        membershipChangeState = MembershipchangeState.REGULAR;
                    }
                }
            }
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
        int term = Integer.parseInt(Objects.requireNonNullElse(msg.query("term"), "0"));
        //        If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
        if (term > currentTerm) {
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
                            if (membershipChangeState == MembershipchangeState.REGULAR)
                                votesForMe++;
                            else {
                                String sender = msg.queryHeader("sender");
                                if (clusterOld.contains(sender))
                                    votesForMeOld++;
                                if (clusterNew.contains(sender))
                                    votesForMeNew++;
                            }

                        }
                        if (membershipChangeState == MembershipchangeState.REGULAR) {
                            if (votesForMe > (cluster.size() + 1) / 2.)
                                wonElection();
                        }
                        if (membershipChangeState == MembershipchangeState.IN_JOINT_CONSENSUS) {
                            if ((votesForMeOld > (clusterOld.size() + 1) / 2.) && (votesForMeNew > (clusterNew.size() + 1) / 2.))
                                wonElection();
                        }

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
                    case "clientPut": {
//                        relay to leader
                        Message response = new Message();
                        response.addHeader("type", "clientPutResponse");
                        response.add("success", 0);
                        response.add("leaderId", leaderId);
                        sendBlindly(response, msg.queryHeader("sender"));
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
                            String sender = msg.queryHeader("sender");
                            String uuid = msg.query("uuid");
                            Pair<String, Boolean> lastOp = lastUuids.get(sender);
                            // already successfully executed earlier
                            if (lastOp != null && lastOp.first.equals(uuid) && lastOp.second) {
                                Message successMessage = new Message();
                                successMessage.addHeader("type", "clientPutResponse");
                                successMessage.add("success", 1);
                                successMessage.add("entries", msg.query("entries"));
                                sendBlindly(successMessage, sender);
                                lastUuids.get(sender).second = true;
                                return;
                            }
                            List<LogEntry> entries = Arrays.asList(mapper.readValue(msg.query("entries"), LogEntry[].class));
//                            set correct terms
                            for (LogEntry entry : entries) {
                                entry.setTerm(currentTerm);
                                if (entry.getKey().equals("noop")) {
                                    Message denied = new Message();
                                    denied.addHeader("type", "clientPutResponse");
                                    denied.add("success", 0);
                                    sendBlindly(denied, sender);
                                    return;
                                }
                            }
                            synchronized (logLock) {
                                int startIndex = log.size();
                                int endIndex = startIndex + entries.size() - 1;
                                log.addAll(entries);
                                clientIndices.put(sender, new Pair<>(startIndex, endIndex));
                                lastUuids.put(sender, new Pair<>(uuid, false));
                            }
                            System.out.println("leader received " + entries.size() + " entries from " + msg.queryHeader("sender"));
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    }
                    case "clientGet": {
                        String sender = msg.queryHeader("sender");
                        String key = msg.query("key");
                        synchronized (getQueueLock) {
                            getQueue.offer(new GetRequest(sender, key));
                        }
                        break;
                    }
                    case "configChange": {
                        try {
                            setCluster(Arrays.asList(mapper.readValue(msg.query("cluster"), String[].class)), clusterNew);
                            nonVotingMembers = clusterNew.stream().filter(item -> cluster.contains(item)).toList();
                            if (nonVotingMembers.isEmpty())
                                membershipChangeState = MembershipchangeState.IN_JOINT_CONSENSUS;
                            else
                                membershipChangeState = MembershipchangeState.NEW_MEMBERS_CATCH_UP;
                            membersToReplicateTo = Stream.concat(cluster.stream(), nonVotingMembers.stream()).toList();
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
//                            avoid out of bounds
                            nextIndex.put(sender, Math.max(nextIndex.get(sender) - 1, 0));
                            sendHeartBeat(sender);
                            return;
                        }
//                        successful
                        int _matchIndex = Math.min(Integer.parseInt(msg.query("matchIndex")), log.size() - 1);
                        matchIndex.put(sender, _matchIndex);
                        nextIndex.put(sender, _matchIndex + 1);
                        updateLeaderCommitIndex();
                        synchronized (getQueueLock) {
                            if (getQueue.peek() != null) {
                                GetRequest getRequest = getQueue.peek();
                                getRequest.increment();
                                if (getRequest.getCounter() > (cluster.size() + 1) / 2.) {
                                    getRequest = getQueue.poll();
                                    Message reply = new Message();
                                    reply.addHeader("type", "clientGetResponse");
                                    String key = getRequest.getKey();
                                    reply.add("key", key);
                                    reply.add("value", stateMachine.get(key));
                                    reply.add("success", 1);
                                    sendBlindly(reply, getRequest.getClient());
                                }
                            }
                        }
                        break;
                    }
                    case "changeConfig": {
                        try {
                            String cNew = msg.query("newConfig");
                            clusterNew = Arrays.asList(mapper.readValue(cNew, String[].class));
                            List<List<String>> temp = new ArrayList<>();
                            temp.add(cluster);
                            temp.add(clusterNew);
                            String cOldNew = mapper.writeValueAsString(temp);
                            LogEntry entry = new LogEntry(currentTerm, "config:cOldNew", cOldNew);
                            synchronized (logLock) {
                                log.add(entry);
                                clusterOld = cluster;
                                cluster = null;
                            }
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
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
            synchronized (messageLock) {
                try {
                    while (queue.peek() == null)
                        messageLock.wait();
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

    public void commit(int fromIndex, int toIndex) {
        System.out.println(this.id + " commited from " + fromIndex + " to " + toIndex);
        for (int i = fromIndex; i <= toIndex; i++) {
            LogEntry e = log.get(i);
            stateMachine.put(e.getKey(), e.getValue());
        }
    }

    private boolean isCommitMajority(int commitIndex) {
        if (membershipChangeState == MembershipchangeState.IN_JOINT_CONSENSUS) {
            int oldCounter = 1;
            int newCounter = 1;
            for (String member : clusterOld) {
                if (matchIndex.get(member) >= commitIndex) {
                    oldCounter++;
                }
            }
            for (String member : clusterNew) {
                if (matchIndex.get(member) >= commitIndex) {
                    newCounter++;
                }
            }
            return (oldCounter > (clusterOld.size() + 1) / 2.) && (newCounter > (clusterNew.size() + 1) / 2.);
        }
        //        if leader is not part of new config, do not count server for majority
        int counter = cluster.contains(this.id) ? 1 : 0;
        int addLeader = counter;
        for (String member : cluster) {
            if (matchIndex.get(member) >= commitIndex) {
                counter++;
            }
        }
        return counter > (cluster.size() + addLeader) / 2.;
    }

    public void updateLeaderCommitIndex() {
        for (int nextCommitIndex = log.size() - 1; nextCommitIndex > commitIndex; nextCommitIndex--) {
            if (isCommitMajority(nextCommitIndex)) {
                commit(commitIndex, nextCommitIndex);
                commitIndex = nextCommitIndex;
                System.out.println("leader: new commitIndex " + commitIndex);
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
                    lastUuids.get(client).second = true;
                }
//                cOldNew config is commited
                if (membershipChangeState == MembershipchangeState.IN_JOINT_CONSENSUS && commitIndex >= cOldNewIndex) {
                    synchronized (logLock) {
                        try {
                            log.add(new LogEntry(currentTerm, "config:cNew", mapper.writeValueAsString(clusterNew)));
                            cluster = clusterNew;
                            clusterOld = null;
                            clusterNew = null;
                            cNewIndex = log.size() - 1;
                            membershipChangeState = MembershipchangeState.REGULAR;
                            cOldNewIndex = -1;
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                if (membershipChangeState == MembershipchangeState.REGULAR && cNewIndex != -1 && commitIndex >= cNewIndex) {
                    cNewIndex = -1;
                    if (!cluster.contains(this.id)) {
                        serverType = ServerType.FOLLOWER;
                        heartbeatThread.interrupt();
                        heartbeatThread = null;
                    }
                }
                return;
            }
        }
    }

    @Override
    protected void engage() {
        new Thread(electionTimeout).start();
        new Thread(processMessages).start();
        while (true) {
            Message m = receive();
            synchronized (messageLock) {
                queue.offer(m);
                messageLock.notify();
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
