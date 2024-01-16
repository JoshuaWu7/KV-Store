package com.s82033788.CPEN431.A4;

import com.google.protobuf.ByteString;

public class UnwrappedMessage {
    ByteString reqID;
    ByteString payload;
    long crc;

    public UnwrappedMessage(ByteString reqID, ByteString payload, long crc) {
        this.reqID = reqID;
        this.payload = payload;
        this.crc = crc;
    }
}
