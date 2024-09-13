package de.kbartz;

public class LogEntry {

    private int term;
    private final String key;
    private final String value;

    public LogEntry() {
        term = -1;
        key = "";
        value = "";
    }

    public LogEntry(String key, String value) {
        this.key = key;
        this.value = value;
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

    public void setTerm(int term) {
        this.term = term;
    }

    public String toString() {
        return "Term: " + term + ", Key: " + key + ", Value: " + value;
    }

    @Override
    public boolean equals(Object logEntry) {
        if (!(logEntry instanceof LogEntry other)) return false;
        return other.term == term && other.key.equals(key) && other.value.equals(value);
    }
}
