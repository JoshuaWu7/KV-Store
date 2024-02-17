package com.g7.CPEN431.A7.newProto;

public interface KVRequest {
boolean hasCommand();
int getCommand();
void setCommand(int command);
boolean hasKey();
byte[] getKey();
void setKey(byte[] key);
boolean hasValue();
byte[] getValue();
void setValue(byte[] value);
boolean hasVersion();
int getVersion();
void setVersion(int version);
}
