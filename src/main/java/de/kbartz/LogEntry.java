package de.kbartz;

public class LogEntry {

    private final int term;
    private final String key;
    private final String value;

    public LogEntry() {
        term = -1;
        key = null;
        value = null;
    }

    public LogEntry(int term, String key, String value) {
        this.term = term;
        this.key = key;
        this.value = value;
    }

    public int getTerm() {
        return term;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
