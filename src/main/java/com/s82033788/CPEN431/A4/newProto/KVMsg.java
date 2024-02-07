package com.s82033788.CPEN431.A4.newProto;

public interface KVMsg {
boolean hasMessageID();
byte[] getMessageID();
void setMessageID(byte[] messageID);
boolean hasPayload();
byte[] getPayload();
void setPayload(byte[] payload);
boolean hasCheckSum();
long getCheckSum();
void setCheckSum(long checkSum);
}
