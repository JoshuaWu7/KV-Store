package com.s82033788.CPEN431.A4;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

public class KVServerThread extends Thread{
    protected DatagramSocket socket = null;

    public KVServerThread() throws IOException {
        this("KVServer");
    }

    public KVServerThread(String kvServer) throws IOException {
        super(kvServer);
        this.socket = new DatagramSocket(13788);
    }

    public void run() {

        while(true)
        {
            byte[] inBuf = new byte[16384];

            try {
                DatagramPacket inPkt = new DatagramPacket(inBuf, inBuf.length);
                socket.receive(inPkt);
            } catch (IllegalArgumentException e) {
                System.err.println("Incoming packet size exceeded, will not process packet");
                System.err.println(e.getMessage());
            }  catch (IOException e) {
                System.err.println("IO Socket error");
                System.err.println(e.getMessage());
            }
        }

    }
}
