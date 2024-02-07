package com.s82033788.CPEN431.A4.wrappers;

import com.google.protobuf.ByteString;
import com.s82033788.CPEN431.A4.newProto.KVMsg;

public class UnwrappedMessage implements KVMsg {

    private ByteString reqID;
    private byte[] msgID;
    private byte[] payload;
    private long crc;


//    public UnwrappedMessage(ByteString reqID, PublicBuffer payload, long crc) {
//        this.setReqID(reqID);
//        this.setPayload(payload);
//        this.setCrc(crc);
//    }


    public UnwrappedMessage(byte[] msgID, byte[] payload, long crc) {
        this.msgID = msgID;
        this.payload = payload;
        this.crc = crc;
    }

    public UnwrappedMessage() {
    }

    @Override
    public boolean hasMessageID() {
        return reqID == null;
    }

    @Override
    public byte[] getMessageID() {
        return msgID;
    }

//    public ByteString getMsgIDString() {
//        return reqID;
//    }

    @Override
    public void setMessageID(byte[] messageID) {
        this.msgID = messageID;
        this.reqID = ByteString.copyFrom(messageID);
    }

    @Override
    public boolean hasPayload() {
        return this.payload != null;
    }

    @Override
    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    @Override
    public boolean hasCheckSum() {
        return true;
    }

    @Override
    public long getCheckSum() {
        return crc;
    }

    @Override
    public void setCheckSum(long checkSum) {
        crc = checkSum;
    }

    @Override
    public byte[] getPayload() {
        return this.payload;
    }
}
