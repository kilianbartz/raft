package de.kbartz;

public class GetRequest {

    private final String client;
    private final String key;
    private int counter;

    public GetRequest(String client, String key) {
        if (client == null || key == null)
            throw new IllegalArgumentException("both arguments must not be null");
        this.client = client;
        this.key = key;
        this.counter = 1;
    }

    public void increment() {
        counter++;
    }

    public int getCounter() {
        return counter;
    }

    public String getClient() {
        return client;
    }

    public String getKey() {
        return key;
    }
}
