package com.g7.CPEN431.A7.newProto.KVRequest;

public interface PutPair {
boolean hasKey();
byte[] getKey();
void setKey(byte[] key);
boolean hasValue();
byte[] getValue();
void setValue(byte[] value);
}
