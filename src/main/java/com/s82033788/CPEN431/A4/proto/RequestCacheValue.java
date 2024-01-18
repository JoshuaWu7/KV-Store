package com.s82033788.CPEN431.A4.proto;

import java.net.DatagramPacket;

public class RequestCacheValue {
    public DatagramPacket result;
    public long incomingCRC;

    public RequestCacheValue(DatagramPacket result, long incomingCRC) {
        this.result = result;
        this.incomingCRC = incomingCRC;
    }
}
