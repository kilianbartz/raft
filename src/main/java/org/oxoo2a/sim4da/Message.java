package org.oxoo2a.sim4da;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Message {

    public Message () {
        addCategory("Header");
        addCategory("Payload");
    }

    public Message ( Message other ) {
        content = new HashMap<>();
        content.putAll(other.content);
        for (String category : other.content.keySet()) {
            content.put(category,new HashMap<>());
            content.get(category).putAll(other.content.get(category));
        }
    }
    public void addCategory ( String category ) {
        if (content.containsKey(category)) {
            System.err.println("Adding category " + category + " twice! Ignoring.");
            return;
        }
        content.put(category,new HashMap<>());
    }

    public void removeCategory ( String category ) {
        if (category.equals("Header") || category.equals("Payload")) {
            System.err.println("Cannot remove category " + category + "!");
            return;
        }
        if (!content.containsKey(category)) {
            System.err.println("Removing non-existent category " + category + "!");
            return;
        }
        content.remove(category);
    }

    public Message addWithCategory(String category, String key, String value ) {
        content.get(category).put(key,value);
        return this;
    }
    public Message add ( String key, String value ) {
        return addWithCategory("Payload",key,value);
    }

    public Message add ( String key, int value ) {
        return addWithCategory("Payload",key,String.valueOf(value));
    }

    public Message addHeader ( String key, String value ) {
        return addWithCategory("Header",key,value);
    }

    public Message addHeader ( String key, int value ) {
        return addWithCategory("Header",key,String.valueOf(value));
    }

    public String queryWithCategory(String category, String key ) {
        return content.get(category).get(key);
    }

    public String query ( String key ) {
        return queryWithCategory("Payload",key);
    }

    public int queryInteger ( String key ) {
        return Integer.parseInt(queryWithCategory("Payload",key));
    }

    public String queryHeader ( String key ) {
        return queryWithCategory("Header",key);
    }

    public int queryHeaderInteger ( String key ) {
        return Integer.parseInt(queryWithCategory("Header",key));
    }

    public Map<String,String> getPayload () {
        return content.get("Payload");
    }

    public Map<String,String> getHeader () {
        return content.get("Header");
    }


    public String toJson () throws JsonProcessingException {
        return serializer.writeValueAsString(this);
    }

    public static Message fromJson ( String s ) throws IOException {
        return serializer.readValue(s,Message.class);
    }

    @Override
    public String toString () {
        String result;
        try {
            result = toJson();
        } catch (JsonProcessingException e) {
            result = "Unable to serialize message";
        }
        return result;
    }

    private Map<String, Map<String,String>> content = new HashMap<>();
    private static final ObjectMapper serializer = new ObjectMapper();
}
