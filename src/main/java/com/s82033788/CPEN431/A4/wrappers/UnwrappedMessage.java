package com.s82033788.CPEN431.A4.wrappers;

import com.google.protobuf.ByteString;

public class UnwrappedMessage {
    private ByteString reqID;
    private ByteString payload;
    private long crc;

    public UnwrappedMessage(ByteString reqID, ByteString payload, long crc) {
        this.setReqID(reqID);
        this.setPayload(payload);
        this.setCrc(crc);
    }

    public ByteString getReqID() {
        return reqID;
    }

    public void setReqID(ByteString reqID) {
        this.reqID = reqID;
    }

    public ByteString getPayload() {
        return payload;
    }

    public void setPayload(ByteString payload) {
        this.payload = payload;
    }

    public long getCrc() {
        return crc;
    }

    public void setCrc(long crc) {
        this.crc = crc;
    }
}
