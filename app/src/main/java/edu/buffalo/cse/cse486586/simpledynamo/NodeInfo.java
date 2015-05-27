package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by abhinav on 4/25/15.
 */
public class NodeInfo
{
    int port_number;
    String hash;
    int avd_id;

    public NodeInfo(int port_number, String hash)
    {
        this.port_number = port_number;
        this.hash = hash;
        this.avd_id = (port_number/2);
    }
}
