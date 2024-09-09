package org.oxoo2a.sim4da;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkConnection {


    public NetworkConnection(String node_name ) {
        this.node_name = node_name;
        logger = LoggerFactory.getLogger(node_name);
        peer = new NodeProxy(this);
        network.registerConnection(this,peer);
    }

    public String NodeName () {
        return node_name;
    }

    public void engage ( Runnable node_main ) {
        this.node_main = node_main;
        thread = new Thread(this::node_main_base);
        thread.start();
    }

    private void node_main_base() {
        simulator.awaitSimulationStart();
        if (simulator.isSimulating())
            node_main.run();
    }

    public void join () {
        try {
            thread.join();
        }
        catch (InterruptedException e) {}
    }

    public Message receive () {
        Message m = network.receive(this);
        logger.debug("Received message from "+m.queryHeader("sender"));
        return m;
    }

    public void send ( Message message, String to_node_name ) throws UnknownNodeException {
        logger.debug("Sending message to "+to_node_name);
        network.send(message, this, to_node_name);
    }

    public void sendBlindly ( Message message, String to_node_name ) {
        try {
            send(message, to_node_name);
        }
        catch (Exception e) {}
    }

    public void send ( Message message ) {
        logger.debug("Broadcasting message");
        network.send(message, this);
    }

    public Logger getLogger() {
        return logger;
    }

    private final String node_name;
    private final Simulator simulator = Simulator.getInstance();
    private final Network network = Network.getInstance();
    private Thread thread = null;
    private final NodeProxy peer;
    private final Logger logger;
    private Runnable node_main = null;
}
