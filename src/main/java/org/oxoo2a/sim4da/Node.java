package org.oxoo2a.sim4da;

public class Node {

    public Node ( String name ) {
        nc = new NetworkConnection(name);
        nc.engage(this::engage);
    }

    protected void engage () {
        nc.getLogger().debug("Engaging node, but no code defined");
    }

    protected void send ( Message message, String to_node_name ) throws UnknownNodeException {
        nc.send(message, to_node_name);
    }

    protected void sendBlindly ( Message message, String to_node_name ) {
        nc.sendBlindly(message, to_node_name);
    }


    protected void broadcast ( Message message ) {
        nc.send(message);
    }

    protected Message receive () {
        return nc.receive();
    }

    protected String NodeName () {
        return nc.NodeName();
    }

    protected void sleep ( int millis ) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
        }
    }
    private NetworkConnection nc = null;
}
