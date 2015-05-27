package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;

public class Message implements Serializable
{
    int sender_port;
    String key;
    String value;
    MessageType msg_type;
    HashMap<String, String> data = new HashMap<String, String>();

    public Message(int sender_port)
    {
        this.sender_port = sender_port;
    }
}
