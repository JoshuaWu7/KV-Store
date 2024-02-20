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
boolean hasServerAddress();
byte[] getServerAddress();
void setServerAddress(byte[] serverAddress);
boolean hasServerPort();
int getServerPort();
void setServerPort(int serverPort);
boolean hasInformationTime();
long getInformationTime();
void setInformationTime(long informationTime);
}
