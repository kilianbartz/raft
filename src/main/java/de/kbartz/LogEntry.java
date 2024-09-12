package de.kbartz;

public class LogEntry {

    private int term;
    private final String key;
    private final String value;

    public LogEntry() {
        term = -1;
        key = null;
        value = null;
    }

    public LogEntry(String key, String value) {
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

    public void setTerm(int term) {
        this.term = term;
    }
}
