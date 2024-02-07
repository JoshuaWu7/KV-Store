package com.s82033788.CPEN431.A4.newProto;

public interface KVResponse {
boolean hasErrCode();
int getErrCode();
void setErrCode(int errCode);
boolean hasValue();
byte[] getValue();
void setValue(byte[] value);
boolean hasPid();
int getPid();
void setPid(int pid);
boolean hasVersion();
int getVersion();
void setVersion(int version);
boolean hasOverloadWaitTime();
int getOverloadWaitTime();
void setOverloadWaitTime(int overloadWaitTime);
boolean hasMembershipCount();
int getMembershipCount();
void setMembershipCount(int membershipCount);
}
