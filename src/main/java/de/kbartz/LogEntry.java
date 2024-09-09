package de.kbartz;

public class LogEntry {

    private int term;
    private String key;
    private String value;

    public LogEntry(int term, String key, String value) {
        this.term = term;
        this.key = key;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
